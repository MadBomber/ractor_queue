#!/usr/bin/env ruby
# frozen_string_literal: true

$stdout.sync = true

# examples/03_rag_pipeline.rb
#
# A dual-pipeline RAG (Retrieval-Augmented Generation) system built on RactorQueue.
#
# Two concurrent pipelines share a single sqlite-vec vector database:
#   INGEST:  Reader → Chunkers×N → Embedders×N → Writer
#   QUERY:   Main   → QueryEmbedder → Main
#
# A ratatui_ruby TUI shows ingestion progress and accepts live semantic queries.
#
# Usage:
#   bundle exec ruby examples/03_rag_pipeline.rb /path/to/docs
#   bundle exec ruby examples/03_rag_pipeline.rb /path/to/docs --db ./my_index.db
#
# Requirements:
#   bundle exec rake compile   (native extension must be compiled first)

$LOAD_PATH.unshift File.expand_path("../lib", __dir__)
require "ractor_queue"
require "sqlite3"
require "sqlite_vec"
require "informers"
require "ratatui_ruby"
require "etc"
require "tmpdir"
require "optparse"

# ── Aliases ────────────────────────────────────────────────────────────────────
C = RatatuiRuby::Layout::Constraint
L = RatatuiRuby::Layout::Layout
W = RatatuiRuby::Widgets

# ── Embedding model ────────────────────────────────────────────────────────────
MODEL_NAME    = "sentence-transformers/all-MiniLM-L6-v2"
EMBEDDING_DIM = 384

# ── Pipeline tuning ────────────────────────────────────────────────────────────
BATCH_SIZE    = 50    # Writer commits every N EmbeddedChunks
CHUNK_SIZE    = 512   # characters per chunk
CHUNK_OVERLAP = 64    # character overlap between adjacent chunks
MIN_CHUNK     = 64    # discard tail chunks shorter than this
SPIN_DRAIN    = 50    # max progress events drained per TUI frame

N_EMBEDDERS   = (Etc.nprocessors / 2).clamp(2, 6)
N_CHUNKERS    = 2

# ── Queue capacities ───────────────────────────────────────────────────────────
DOC_QUEUE_CAP      = 64
CHUNK_QUEUE_CAP    = 256
EMBED_QUEUE_CAP    = 128
PROGRESS_QUEUE_CAP = 512
QUERY_JOBS_CAP     = 8
QUERY_RESULTS_CAP  = 8

# ── Queue element Structs (all instances must be Ractor.make_shareable'd) ──────
#
# Note: :source_path is added to RawChunk and EmbeddedChunk to populate
# doc_meta.source_path in sqlite-vec. This is additive vs. the design spec.
RawDocument   = Struct.new(:id, :path, :text)
RawChunk      = Struct.new(:doc_id, :chunk_index, :text, :source_path)
EmbeddedChunk = Struct.new(:doc_id, :chunk_index, :text, :vector_blob, :source_path)
ProgressEvent = Struct.new(:stage, :count, :detail)
QueryJob      = Struct.new(:id, :text)
Hit           = Struct.new(:text, :score)
QueryResult   = Struct.new(:query_id, :query_text, :hits)

# ── TUI-local state (lives in Main Ractor only, never shared) ─────────────────
AppState = Struct.new(
  :total_docs,      # Integer — set by Reader's first ProgressEvent(:total,...)
  :docs_read,       # Integer
  :chunks_produced, # Integer
  :embeddings_done, # Integer
  :stored_count,    # Integer
  :ingest_done,     # Boolean
  :activity_log,    # Array(String) — circular buffer, last 3 entries
  :embed_times,     # Array(Float)  — rolling window for rate calculation
  :input_buffer,    # String        — current query being typed
  :current_result,  # QueryResult | nil
  :layout,          # :horizontal | :vertical
  keyword_init: true
)

# ── Text chunking ──────────────────────────────────────────────────────────────

# Split +text+ into overlapping fixed-size chunks, splitting on whitespace.
# Returns an Array of Strings, each between MIN_CHUNK and CHUNK_SIZE characters.
def chunk_text(text, size: CHUNK_SIZE, overlap: CHUNK_OVERLAP, min: MIN_CHUNK)
  chunks = []
  start  = 0
  while start < text.length
    stop = [start + size, text.length].min
    if stop < text.length
      # Walk back to a whitespace boundary so we don't cut mid-word
      ws = text.rindex(/\s/, stop)
      stop = ws if ws && ws > start + min
    end
    chunk = text[start...stop]
    chunks << chunk if chunk.length >= min
    step  = stop - overlap
    start = step > start ? step : stop  # guard: always advance
  end
  chunks
end

if __FILE__ == $PROGRAM_NAME && ARGV.first == "--test-chunk"
  sample = ("word " * 200).strip
  chunks = chunk_text(sample)
  raise "expected multiple chunks" unless chunks.size > 1
  raise "chunk too long" if chunks.any? { |c| c.length > CHUNK_SIZE + 10 }
  raise "overlap not working" if chunks.size > 1 && !chunks[0][-CHUNK_OVERLAP..]&.then { |tail| chunks[1].start_with?(tail.split.first || "") }
  puts "chunk_text: ok (#{chunks.size} chunks from #{sample.length} chars)"
  exit 0
end

# ── Database helpers ───────────────────────────────────────────────────────────

# Open (or create) the sqlite-vec database and ensure schema exists.
# Called by Main before spawning any Ractors.
def setup_db(path)
  db = SQLite3::Database.new(path)
  db.enable_load_extension(true)
  SqliteVec.load(db)
  db.enable_load_extension(false)
  db.execute("PRAGMA journal_mode=WAL")
  db.execute(<<~SQL)
    CREATE VIRTUAL TABLE IF NOT EXISTS vec_docs USING vec0(
      embedding float[#{EMBEDDING_DIM}] distance_metric=cosine
    )
  SQL
  db.execute(<<~SQL)
    CREATE TABLE IF NOT EXISTS doc_meta (
      rowid       INTEGER PRIMARY KEY AUTOINCREMENT,
      chunk_text  TEXT NOT NULL,
      source_path TEXT NOT NULL
    )
  SQL
  db.close
end

# Open an existing database for writing (called inside Writer Ractor).
def open_write_db(path)
  db = SQLite3::Database.new(path)
  db.enable_load_extension(true)
  SqliteVec.load(db)
  db.enable_load_extension(false)
  db.execute("PRAGMA journal_mode=WAL")
  db
end

# Open an existing database for reading (called inside QueryEmbedder Ractor).
def open_read_db(path)
  db = SQLite3::Database.new(path)
  db.enable_load_extension(true)
  SqliteVec.load(db)
  db.enable_load_extension(false)
  db
end

# Commit a batch of EmbeddedChunk structs to both tables.
# Returns the number of rows committed.
def insert_batch(db, batch)
  return 0 if batch.empty?
  db.transaction do
    batch.each do |chunk|
      db.execute(
        "INSERT INTO doc_meta(chunk_text, source_path) VALUES (?, ?)",
        [chunk.text, chunk.source_path]
      )
      rowid = db.last_insert_row_id
      db.execute(
        "INSERT INTO vec_docs(rowid, embedding) VALUES (?, ?)",
        [rowid, chunk.vector_blob]
      )
    end
  end
  batch.size
end

# Run a KNN search. Returns an Array of frozen Hit structs.
def knn_search(db, vector_blob, limit: 10)
  rows = db.execute(<<~SQL, [vector_blob, limit])
    SELECT m.chunk_text, m.source_path, v.distance
    FROM vec_docs v
    JOIN doc_meta m ON m.rowid = v.rowid
    WHERE v.embedding MATCH ? AND k=?
    ORDER BY v.distance
  SQL
  rows.map do |chunk_text, _source_path, distance|
    score = (1.0 - distance / 2.0).clamp(0.0, 1.0)
    Hit.new(chunk_text.freeze, score).freeze
  end
end

if __FILE__ == $PROGRAM_NAME && ARGV.first == "--test-db"
  require "tempfile"
  tmp = Tempfile.new(["rag_test", ".db"])
  tmp.close
  setup_db(tmp.path)
  db = open_write_db(tmp.path)
  fake_vec  = ([0.1] * EMBEDDING_DIM).pack("f*")
  fake_blob = fake_vec.freeze
  fake      = EmbeddedChunk.new("d1", 0, "hello world", fake_blob, "test.txt")
  inserted  = insert_batch(db, [fake])
  raise "expected 1 inserted" unless inserted == 1
  db.close
  rdb  = open_read_db(tmp.path)
  hits = knn_search(rdb, fake_blob, limit: 5)
  raise "expected 1 hit" unless hits.size == 1
  raise "score out of range" unless hits.first.score.between?(0.0, 1.0)
  rdb.close
  tmp.unlink
  puts "DB helpers: ok"
  exit 0
end
