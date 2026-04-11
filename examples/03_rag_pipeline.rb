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
