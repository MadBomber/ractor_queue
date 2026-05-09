#!/usr/bin/env ruby
# frozen_string_literal: true

$stdout.sync = true

# examples/05_simd.rb
#
# SIMD Pattern: the same CPU-bound function (TF-IDF scoring) applied to N
# documents in parallel across W Ractor workers via RactorQueue.
#
# Why Ractors beat Threads for this workload:
#   TF-IDF scoring is pure Ruby string/hash work — 100% CPU-bound.
#   Threads share the GVL: only one thread runs Ruby at a time on CPU work.
#   Ractors each have their own GVL: W Ractors run W scoring jobs in parallel.
#   RactorQueue is the only Ruby queue that is always Ractor.shareable?.
#
# Run:
#   bundle exec ruby examples/05_simd.rb              # synthetic 20k-doc corpus
#   bundle exec ruby examples/05_simd.rb /path/to/dir # load .txt files from dir

require "etc"
require_relative "parallel"

WORKERS   = Etc.nprocessors
SEPARATOR = "=" * 62

def bench
  t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  yield
  Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0
end

# ─────────────────────────────────────────────────────────────────────────────

puts
puts "TF-IDF Scoring — SIMD Pattern"
puts SEPARATOR
puts "  Workers: #{WORKERS} Ractors  (#{Etc.nprocessors} logical CPU cores)"

corpus = Compute.load_corpus(ARGV[0])
puts "  Corpus:  #{corpus.size} documents"

# Build the IDF table once from the full corpus, then freeze it.
# Workers receive the frozen table via the fn_q workaround inside Parallel.map.
idf      = Compute.build_idf(corpus).freeze
tfidf_fn = ->(doc) { Compute.tfidf(doc, idf) }

puts
puts "  Running serial baseline..."
# Store checksums (Fixnums) not full Hash results — keeping 20k Hashes alive
# during the parallel run would fragment the heap and trigger extra GC cycles,
# unfairly penalising the parallel benchmark. Checksums give the same
# correctness guarantee (hash equality) with zero GC overhead.
serial_top    = nil
serial_checks = nil
t_serial = bench do
  results        = corpus.map { |doc| tfidf_fn.call(doc) }
  serial_top     = results[0].max_by(5) { |_, v| v }
  serial_checks  = results.map(&:hash)
  # `results` goes out of scope here — 20k Hashes freed before the parallel run
end

puts "  Running parallel (#{WORKERS} Ractors)..."
parallel_scores = nil
t_parallel = bench { parallel_scores = Parallel.map(corpus, workers: WORKERS, fn: tfidf_fn) }

# ── Results ───────────────────────────────────────────────────────────────────

puts
puts "  Serial:   #{format('%.3f', t_serial)}s"
puts "  Parallel: #{format('%.3f', t_parallel)}s   (#{format('%.1f', t_serial / t_parallel)}x speedup)"

puts
puts "  Top 5 terms by TF-IDF score in doc[0]:"
serial_top.each { |term, score| printf "    %-22s %.4f\n", term, score }

# Verify correctness: parallel results must match serial checksums exactly
mismatches = serial_checks.zip(parallel_scores).count { |s, p| p.hash != s }
puts
if mismatches > 0
  warn "  WARNING: #{mismatches}/#{corpus.size} documents have mismatched results!"
else
  puts "  Correctness: all #{corpus.size} documents match serial output. OK"
end
