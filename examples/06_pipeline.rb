#!/usr/bin/env ruby
# frozen_string_literal: true

$stdout.sync = true

# examples/06_pipeline.rb
#
# MIMD Pipeline Pattern: 20k documents flow through a 2-stage pipeline,
# one document per queue slot.
#
#   Stage 0 — Chunk+Vectorise: chunk the doc into overlapping windows,
#             compute TF vectors for every chunk → Array<[chunk_text, tf_hash]>
#   Stage 1 — Rank: score each vectorised chunk against the query vector via
#             cosine similarity → [best_score, best_chunk_text]
#
# Processing at document granularity (not chunk granularity) keeps queue
# pressure low — only 20k items travel through the pipeline — while giving
# each Ractor enough CPU work per pop (~25 chunks per document) to outrun
# the queue coordination overhead.
#
# RactorQueue is the only queue that connects Ractor workers across stages —
# Ruby's built-in Queue uses Mutex internally and is not Ractor.shareable?.
#
# Run:
#   bundle exec ruby examples/06_pipeline.rb              # synthetic corpus
#   bundle exec ruby examples/06_pipeline.rb /path/to/dir # load .txt files

require "etc"
require_relative "parallel"

# Half of logical CPUs is the empirical sweet spot for this 2-stage pipeline:
# more workers → queue contention overwhelms parallelism on M-series chips.
WORKERS   = (Etc.nprocessors / 2).clamp(4, 8)
QUERY     = "ruby concurrency ractor".freeze
TOP_N     = 5
SEPARATOR = "=" * 62

def bench
  t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  yield
  Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0
end

# ─────────────────────────────────────────────────────────────────────────────

puts
puts "Semantic Chunk Ranking — Pipeline (MIMD) Pattern"
puts SEPARATOR
puts "  Workers: #{WORKERS} Ractors per stage  (#{Etc.nprocessors} logical CPU cores)"

corpus = Compute.load_corpus(ARGV[0])
puts "  Corpus:  #{corpus.size} documents"
puts "  Query:   #{QUERY.inspect}"

query_vector = Compute.build_query_vector(QUERY).freeze

# Stage 0: chunk the document and tokenize every chunk.
# Tokenizing (regex scan + stopword filter) is the CPU-heavy step (>60% of serial time).
# Returns Array<[frozen_chunk_text, frozen_tokens_array]> — one entry per window.
# All strings are frozen so the nested structure is fully Ractor.shareable?.
chunk_tokenize_fn = ->(doc) {
  Compute.chunk(doc)
    .map! { |c|
      tokens = Compute.tokenize(c).map!(&:freeze).freeze
      [c.freeze, tokens].freeze
    }
    .tap(&:freeze)
    .freeze
}

# Stage 1: compute TF vectors and rank every chunk against the query,
# return the top-scored [score, chunk_text] pair for this document.
# Receives the output of stage 0 (Array<[chunk_text, tokens]>).
tf_rank_fn = ->(tokenized_chunks) {
  best = nil
  best_score = -1.0
  tokenized_chunks.each do |chunk_text, tokens|
    next if tokens.empty?
    n  = tokens.size.to_f
    # tokens are already frozen strings (from chunk_tokenize_fn), so tally
    # keys are frozen; transform_keys! is unnecessary here.
    tf = tokens.tally.transform_values! { |c| c.fdiv(n).freeze }.freeze
    scored = Compute.rank([chunk_text, tf].freeze, query_vector)
    if scored[0] > best_score
      best_score = scored[0]
      best       = scored
    end
  end
  (best || [0.0.freeze, "".freeze]).freeze
}

# ── Serial baseline ───────────────────────────────────────────────────────────

puts
puts "  Running serial baseline..."
serial_top = nil
t_serial = bench do
  serial_top = corpus.map { |doc|
    best = nil; best_score = -1.0
    Compute.chunk(doc).each do |chunk|
      tokens = Compute.tokenize(chunk)
      next if tokens.empty?
      n  = tokens.size.to_f
      tf = tokens.tally.transform_values! { |c| c.fdiv(n).freeze }
              .transform_keys!(&:freeze).freeze
      scored = Compute.rank([chunk.freeze, tf].freeze, query_vector)
      if scored[0] > best_score; best_score = scored[0]; best = scored; end
    end
    best || [0.0, ""].freeze
  }.max_by(TOP_N, &:first)
end

# ── Parallel pipeline ─────────────────────────────────────────────────────────

puts "  Running parallel pipeline (#{WORKERS} Ractors per stage)..."
parallel_top = nil
t_parallel = bench do
  stages       = [chunk_tokenize_fn, tf_rank_fn]
  parallel_top = Parallel.pipeline(corpus, stages: stages, workers: WORKERS)
                          .max_by(TOP_N, &:first)
end

# ── Output ────────────────────────────────────────────────────────────────────

chunks_per_doc = Compute.chunk(corpus[0]).size
puts
puts "  Serial:   #{format('%.3f', t_serial)}s"
puts "  Parallel: #{format('%.3f', t_parallel)}s   (#{format('%.1f', t_serial / t_parallel)}x speedup)"
puts "  Items processed: #{corpus.size} documents × ~#{chunks_per_doc} chunks/doc"

puts
puts "  Top #{TOP_N} chunks most similar to #{QUERY.inspect}:"
parallel_top.each_with_index do |(score, chunk), i|
  printf "  %d. [%.4f] %s\n", i + 1, score, chunk[0..68]
end

puts
puts "  Note: pipeline result order is not guaranteed (shown after sort by score)."
