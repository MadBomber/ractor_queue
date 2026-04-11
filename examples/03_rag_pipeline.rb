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
require "fileutils"
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

# ── Ractor factories ───────────────────────────────────────────────────────────

# ── Task 5: Reader ─────────────────────────────────────────────────────────────
# Globs dir for .txt/.md/.rb files, pushes ProgressEvent(:total,...) first,
# then RawDocuments, then :shutdown × n_chunkers.
def start_reader(dir, doc_queue, progress_queue, n_chunkers)
  Ractor.new(dir.freeze, doc_queue, progress_queue, n_chunkers) do |dir, doc_queue, progress_queue, n_chunkers|
    files = Dir.glob(File.join(dir, "**", "*.{txt,md,rb}")).sort
    total = files.size
    progress_queue.push(
      Ractor.make_shareable(ProgressEvent.new(:total, total, "found #{total} files in #{dir}"))
    )
    files.each_with_index do |path, i|
      text = File.read(path, encoding: "utf-8", invalid: :replace, undef: :replace)
      doc  = Ractor.make_shareable(RawDocument.new(i, path.freeze, text.freeze))
      doc_queue.push(doc)
      progress_queue.try_push(
        Ractor.make_shareable(ProgressEvent.new(:doc, i + 1, File.basename(path)))
      )
    end
    n_chunkers.times { doc_queue.push(:shutdown) }
  end
end

# ── Task 6: Chunkers ────────────────────────────────────────────────────────────
# N Ractors: pop RawDocuments, split into RawChunks, propagate :shutdown.
def start_chunkers(n, doc_queue, chunk_queue, progress_queue, n_embedders)
  Array.new(n) do
    Ractor.new(doc_queue, chunk_queue, progress_queue, n_embedders) do |doc_queue, chunk_queue, progress_queue, n_embedders|
      chunk_count = 0
      loop do
        item = doc_queue.pop
        break if item.equal?(:shutdown)

        chunks = chunk_text(item.text)
        chunks.each_with_index do |text, idx|
          rc = Ractor.make_shareable(RawChunk.new(item.id, idx, text.freeze, item.path))
          chunk_queue.push(rc)
        end
        chunk_count += chunks.size
        progress_queue.try_push(
          Ractor.make_shareable(
            ProgressEvent.new(:chunk, chunk_count, "chunked #{File.basename(item.path)} (#{chunks.size})")
          )
        )
      end
      # Push one pill per Embedder so every Embedder can receive its own :shutdown.
      n_embedders.times { chunk_queue.push(:shutdown) }
    end
  end
end

# ── Task 7: Embedders ───────────────────────────────────────────────────────────
# N Ractors: each loads its own Informers model, embeds chunks, propagates :shutdown.
def start_embedders(n, chunk_queue, embed_queue, progress_queue)
  Array.new(n) do
    Ractor.new(chunk_queue, embed_queue, progress_queue) do |chunk_queue, embed_queue, progress_queue|
      model = Informers.pipeline("embedding", MODEL_NAME)
      embed_count = 0
      loop do
        item = chunk_queue.pop
        break if item.equal?(:shutdown)

        t0     = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        vector = model.(item.text)
        ms     = ((Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0) * 1000).round
        blob   = vector.pack("f*").freeze

        ec = Ractor.make_shareable(
          EmbeddedChunk.new(item.doc_id, item.chunk_index, item.text, blob, item.source_path)
        )
        embed_queue.push(ec)
        embed_count += 1
        progress_queue.try_push(
          Ractor.make_shareable(
            ProgressEvent.new(:embed, embed_count, "chunk #{embed_count} → #{EMBEDDING_DIM}-dim (#{ms}ms)")
          )
        )
      end
      embed_queue.push(:shutdown)
    end
  end
end

# ── Task 8: Writer ──────────────────────────────────────────────────────────────
# Single Ractor with exclusive write access. Batches inserts; pushes
# ProgressEvent(:done,...) to progress_queue when all Embedders have shut down.
def start_writer(db_path, embed_queue, progress_queue, n_embedders)
  Ractor.new(db_path, embed_queue, progress_queue, n_embedders) do |db_path, embed_queue, progress_queue, n_embedders|
    db        = open_write_db(db_path)
    batch     = []
    total     = 0
    batch_num = 0

    flush = lambda do
      return if batch.empty?
      committed  = insert_batch(db, batch)
      total     += committed
      batch_num += 1
      progress_queue.try_push(
        Ractor.make_shareable(
          ProgressEvent.new(:store, total, "batch ##{batch_num} committed (#{committed} rows)")
        )
      )
      batch.clear
    end

    # Each Embedder pushes exactly one :shutdown pill. Wait for all of them
    # so no in-flight EmbeddedChunks are discarded before the final flush.
    shutdowns_seen = 0
    loop do
      item = embed_queue.pop
      if item.equal?(:shutdown)
        shutdowns_seen += 1
        if shutdowns_seen == n_embedders
          flush.()
          break
        end
      else
        batch << item
        flush.() if batch.size >= BATCH_SIZE
      end
    end

    db.close
    progress_queue.push(
      Ractor.make_shareable(ProgressEvent.new(:done, total, "index complete — #{total} chunks stored"))
    )
  end
end

# ── Task 9: QueryEmbedder ───────────────────────────────────────────────────────
# Single Ractor: loads Informers model + sqlite-vec read handle.
# Embeds query, runs KNN, pushes populated QueryResult.
def start_query_embedder(db_path, query_jobs, query_results)
  Ractor.new(db_path, query_jobs, query_results) do |db_path, query_jobs, query_results|
    model = Informers.pipeline("embedding", MODEL_NAME)
    db    = open_read_db(db_path)

    query_id = 0
    loop do
      job = query_jobs.pop
      break if job.equal?(:shutdown)

      vector = model.(job.text)
      blob   = vector.pack("f*").freeze
      hits   = knn_search(db, blob, limit: 10)
      result = Ractor.make_shareable(
        QueryResult.new(query_id += 1, job.text.freeze, hits.freeze)
      )
      query_results.push(result)
    end

    db.close
  end
end

# ── TUI state helpers ──────────────────────────────────────────────────────────

def initial_state
  size = RatatuiRuby.get_terminal_size
  AppState.new(
    total_docs:      0,
    docs_read:       0,
    chunks_produced: 0,
    embeddings_done: 0,
    stored_count:    0,
    ingest_done:     false,
    activity_log:    [],
    embed_times:     [],
    input_buffer:    +"",
    current_result:  nil,
    layout:          compute_layout(size)
  )
end

# Returns :horizontal (side-by-side) or :vertical (stacked).
# Horizontal for wide/short terminals; vertical for taller ones.
def compute_layout(size)
  return :horizontal if size.width.to_f / size.height > 2.5
  return :horizontal if size.height < 25
  :vertical
end

# ── TUI render ─────────────────────────────────────────────────────────────────

# Drain progress events and query results into state. Called once per frame.
def drain_queues(state, progress_queue, query_results)
  SPIN_DRAIN.times do
    ev = progress_queue.try_pop
    break if ev.equal?(RactorQueue::EMPTY)

    case ev.stage
    when :total
      state.total_docs = ev.count
    when :doc
      state.docs_read = ev.count
    when :chunk
      state.chunks_produced = ev.count
    when :embed
      state.embeddings_done = ev.count
      state.embed_times << Process.clock_gettime(Process::CLOCK_MONOTONIC)
      state.embed_times.shift if state.embed_times.size > 10
    when :store
      state.stored_count = ev.count
    when :done
      state.stored_count = ev.count
      state.ingest_done  = true
    end

    state.activity_log << ev.detail
    state.activity_log.shift if state.activity_log.size > 3
  end

  result = query_results.try_pop
  state.current_result = result unless result.equal?(RactorQueue::EMPTY)
end

# Approximate embedding rate (chunks/s) over the rolling embed_times window.
def embed_rate(state)
  times = state.embed_times
  return 0.0 if times.size < 2
  elapsed = times.last - times.first
  return 0.0 if elapsed <= 0
  (times.size / elapsed).round(1)
end

def render_ingest_panel(frame, area, state)
  total  = [state.total_docs, 1].max
  ratio  = (state.stored_count.to_f / total).clamp(0.0, 1.0)
  pct    = (ratio * 100).round
  rate   = embed_rate(state)
  status = state.ingest_done ? "Done" : "Running"
  label  = "#{pct}%  #{rate} emb/s  [#{status}]"

  lines = [
    "Documents : #{state.docs_read} / #{state.total_docs}",
    "Chunks    : #{state.chunks_produced}",
    "Embeddings: #{state.embeddings_done}",
    "Stored    : #{state.stored_count}",
    "",
    label
  ].join("\n")

  # Split area: text top, gauge bottom row
  text_area, gauge_area = L.split(
    area,
    direction: :vertical,
    constraints: [C.fill(1), C.length(1)]
  )

  border = W::Block.new(
    title: " Ingestion (#{N_EMBEDDERS} embedders, #{N_CHUNKERS} chunkers) ",
    borders: [:all]
  )
  frame.render_widget(W::Paragraph.new(text: lines, block: border), text_area)
  frame.render_widget(W::LineGauge.new(ratio: ratio), gauge_area)
end

def render_query_panel(frame, area, state)
  result   = state.current_result
  prompt   = "▶ #{state.input_buffer}_"
  hint     = "[Enter: submit  q: quit]"

  hit_lines = if result
    result.hits.first(8).map do |h|
      "#{(h.score * 100).round}%  #{h.text.gsub(/\s+/, " ").slice(0, 80)}"
    end.join("\n")
  else
    "(no results yet)"
  end

  content = "#{hint}\n#{prompt}\n\n#{hit_lines}"

  title  = result ? " Query: \"#{result.query_text.slice(0, 30)}\" " : " Semantic Search "
  border = W::Block.new(title: title, borders: [:all])
  frame.render_widget(W::Paragraph.new(text: content, block: border, wrap: true), area)
end

def render_activity_log(frame, area, state)
  text = state.activity_log.last(3).join("  │  ")
  frame.render_widget(W::Paragraph.new(text: text), area)
end

# Render one complete TUI frame. Called inside tui.draw block.
def render_frame(frame, state)
  total_area = frame.area

  if state.layout == :horizontal
    # Wide terminal: side-by-side panels, log strip at bottom
    main_area, log_area = L.split(
      total_area,
      direction: :vertical,
      constraints: [C.fill(1), C.length(2)]
    )
    ingest_area, query_area = L.split(
      main_area,
      direction: :horizontal,
      constraints: [C.percentage(40), C.fill(1)]
    )
  else
    # Tall terminal: stacked panels, log strip at bottom
    ingest_area, query_area, log_area = L.split(
      total_area,
      direction: :vertical,
      constraints: [C.length(9), C.fill(1), C.length(2)]
    )
  end

  render_ingest_panel(frame, ingest_area, state)
  render_query_panel(frame, query_area, state)
  render_activity_log(frame, log_area, state)
end

# ── Entry point ────────────────────────────────────────────────────────────────

def parse_options(argv)
  options = { db: nil }
  parser  = OptionParser.new do |o|
    o.banner = "Usage: #{$PROGRAM_NAME} DOC_DIR [options]"
    o.on("--db PATH", "Persist sqlite-vec DB to PATH (default: temp file)") do |p|
      options[:db] = p
    end
    o.on("-h", "--help") { puts o; exit }
  end
  remaining = parser.parse(argv)
  doc_dir   = remaining.first
  abort parser.banner + "\n\nError: DOC_DIR is required." if doc_dir.nil?
  abort "Error: #{doc_dir} is not a directory." unless File.directory?(doc_dir)
  options[:doc_dir] = doc_dir
  options
end

def run_pipeline(options)
  doc_dir = options[:doc_dir]
  tmp_dir = Dir.mktmpdir("ractor_rag") unless options[:db]
  db_path = (options[:db] || File.join(tmp_dir, "index.db")).freeze

  # ── Queue creation ──────────────────────────────────────────────────────────
  doc_queue      = RactorQueue.new(capacity: DOC_QUEUE_CAP)
  chunk_queue    = RactorQueue.new(capacity: CHUNK_QUEUE_CAP)
  embed_queue    = RactorQueue.new(capacity: EMBED_QUEUE_CAP)
  progress_queue = RactorQueue.new(capacity: PROGRESS_QUEUE_CAP)
  query_jobs     = RactorQueue.new(capacity: QUERY_JOBS_CAP)
  query_results  = RactorQueue.new(capacity: QUERY_RESULTS_CAP)

  # ── Model warm-up: seeds ~/.cache/huggingface/ so Ractor loads are fast ────
  $stderr.puts "Warming embedding model cache (first run may download ~90 MB)..."
  Informers.pipeline("embedding", MODEL_NAME)

  # ── DB schema setup ─────────────────────────────────────────────────────────
  setup_db(db_path)

  # ── Spawn Ractors (Writer first so DB is ready before QueryEmbedder) ───────
  _writer    = start_writer(db_path, embed_queue, progress_queue, N_EMBEDDERS)
  _embedders = start_embedders(N_EMBEDDERS, chunk_queue, embed_queue, progress_queue)
  _chunkers  = start_chunkers(N_CHUNKERS, doc_queue, chunk_queue, progress_queue, N_EMBEDDERS)
  _qe        = start_query_embedder(db_path, query_jobs, query_results)
  _reader    = start_reader(doc_dir, doc_queue, progress_queue, N_CHUNKERS)

  # ── TUI render loop ─────────────────────────────────────────────────────────
  state    = initial_state
  query_id = 0

  RatatuiRuby.guard_io do
    RatatuiRuby.run do |tui|
      loop do
        # ① Drain queues
        drain_queues(state, progress_queue, query_results)

        # ② Render frame
        tui.draw { |frame| render_frame(frame, state) }

        # ③ Poll input (16ms timeout ≈ 60 FPS)
        event = tui.poll_event(timeout: 0.016)
        next unless event

        case event
        in { type: :key, code: "q" }
          break
        in { type: :key, code: "enter" }
          unless state.input_buffer.strip.empty?
            job = Ractor.make_shareable(
              QueryJob.new(query_id += 1, state.input_buffer.strip.freeze)
            )
            query_jobs.try_push(job)
            state.input_buffer.clear
          end
        in { type: :key, code: "backspace" }
          state.input_buffer.chop!
        in { type: :key, code: String => char } if char.length == 1
          state.input_buffer << char
        in { type: :resize }
          size = RatatuiRuby.get_terminal_size
          state.layout = compute_layout(size)
        else
          # other events — continue
        end
      end
    end
  end

  # ── Graceful shutdown ───────────────────────────────────────────────────────
  query_jobs.try_push(:shutdown)
  puts "\nIndexed #{state.stored_count} chunks from #{state.docs_read} documents."
  puts "DB: #{db_path}" if options[:db]
ensure
  FileUtils.rm_rf(tmp_dir) if tmp_dir && !options[:db]
end

# Guard: only run when invoked directly, not when loaded for --test-chunk / --test-db
unless ARGV.first&.start_with?("--test")
  run_pipeline(parse_options(ARGV))
end
