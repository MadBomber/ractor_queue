#!/usr/bin/env ruby
# frozen_string_literal: true

$stdout.sync = true

# examples/02_performance.rb
#
# Ractor throughput and latency benchmarks for the ractor_queue gem.
# All benchmarks use Ractors — the primary use case for RactorQueue.
#
# Ruby's built-in Queue is excluded from every benchmark: it cannot
# be shared across Ractors and has no equivalent here.
#
# Run from the project root:
#   bundle exec ruby examples/02_performance.rb

$LOAD_PATH.unshift File.expand_path("../lib", __dir__)
require "ractor_queue"
require "etc"

SEPARATOR = "-" * 68
ITEMS     = 50_000   # items per benchmark

def bench(label, count: ITEMS)
  t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  yield
  elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0

  ops_per_sec = (count / elapsed).round
  ns_per_op   = ((elapsed / count) * 1_000_000_000).round

  printf "  %-46s  %8s ops/s   %6d ns/op\n",
         label,
         format_ops(ops_per_sec),
         ns_per_op
  ops_per_sec
end

def format_ops(n)
  n.to_s.reverse.gsub(/(\d{3})(?=\d)/, "\\1_").reverse
end

def section(title)
  puts "\n#{SEPARATOR}"
  puts "  #{title}"
  puts SEPARATOR
end

# ─────────────────────────────────────────────────────────────
# 1. Single Producer / Single Consumer (1P1C)
#    Two Ractors — one pushing, one popping.
#    Baseline: how fast can two Ractors exchange items?
# ─────────────────────────────────────────────────────────────

section "1. Ractor 1P1C Throughput (#{ITEMS} items)"

bench("1 producer Ractor / 1 consumer Ractor") do
  q = RactorQueue.new(capacity: 4096)
  p = Ractor.new(q) { |queue| ITEMS.times { |i| queue.push(i) } }
  c = Ractor.new(q) { |queue| ITEMS.times { queue.pop } }
  p.value
  c.value
end

puts <<~NOTE
  NOTE: Ruby's built-in Queue is not shown — it is not Ractor-shareable.
        RactorQueue is the only option for a bounded queue shared across Ractors.
NOTE

# ─────────────────────────────────────────────────────────────
# 2a. Shared-Queue MPMC Scaling — all Ractors fight over one queue.
#     Contention rises with thread count; capped at 4P/4C to avoid
#     scheduler thrashing (Thread.pass spin on a single cache line).
# ─────────────────────────────────────────────────────────────

section "2a. Shared-Queue MPMC Scaling (#{ITEMS} items total, 1 queue)"

[1, 2, 4, 8].each do |n|
  per = ITEMS / n

  bench("#{n}P / #{n}C  (#{n * 2} Ractors, shared queue)") do
    q  = RactorQueue.new(capacity: 4096)
    ps = n.times.map { Ractor.new(q, per) { |queue, count| count.times { |i| queue.push(i) } } }
    cs = n.times.map { Ractor.new(q, per) { |queue, count| count.times { queue.pop } } }
    ps.each(&:value)
    cs.each(&:value)
  end
end

puts <<~NOTE
  NOTE: One queue shared by all producers and consumers. Every atomic CAS
        on the head/tail pointer invalidates that cache line on every other core.
        The exponential backoff in blocking_push/pop (Thread.pass then sleep)
        prevents scheduler thrashing — 8P/8C completes without stalling.
NOTE

# ─────────────────────────────────────────────────────────────
# 2b. Queue-Pool MPMC Scaling — N independent 1P1C queues.
#     Each producer/consumer pair owns one queue sized to hold
#     all of that producer's items — producers never block, so
#     no Thread.pass spin-wait and no scheduler storm.
#     Shows true parallel scaling to 8P/8C (16 Ractors).
# ─────────────────────────────────────────────────────────────

section "2b. Queue-Pool MPMC Scaling (#{ITEMS} items total, N queues)"

[1, 2, 4, 8].each do |n|
  per = ITEMS / n

  bench("#{n}P / #{n}C  (#{n * 2} Ractors, #{n} queues)") do
    pairs = n.times.map do
      q = RactorQueue.new(capacity: per)   # fits all items — producer never blocks
      p = Ractor.new(q, per) { |queue, count| count.times { |i| queue.push(i) } }
      c = Ractor.new(q, per) { |queue, count| count.times { queue.pop } }
      [p, c]
    end
    pairs.each { |p, c| p.value; c.value }
  end
end

puts <<~NOTE
  NOTE: Each producer/consumer pair has its own queue sized to hold all producer
        items, so producers push without ever blocking (no spin-wait, no Thread.pass
        storm). Throughput scales with cores because Ractors run on real OS threads.
        Trade-off: work is statically partitioned — any producer only feeds its
        paired consumer. For dynamic load balancing across workers, use a small
        pool of shared queues (e.g. 4 queues for 16 Ractors) with work chunked
        large enough that producers rarely hit the capacity limit.
NOTE

# ─────────────────────────────────────────────────────────────
# 3. Round-Trip Latency — Ractor Ping-Pong
#    Two Ractors exchange one item at a time via two queues.
#    Measures the full cross-Ractor handoff cost per item.
# ─────────────────────────────────────────────────────────────

PING_COUNT = 5_000

section "3. Ractor Round-Trip Latency — Ping-Pong (#{PING_COUNT} round trips)"

bench("2-Ractor ping-pong (2 queues)", count: PING_COUNT) do
  fwd  = RactorQueue.new(capacity: 64)
  back = RactorQueue.new(capacity: 64)

  responder = Ractor.new(fwd, back) do |fq, bq|
    PING_COUNT.times { bq.push(fq.pop) }
  end

  PING_COUNT.times do |i|
    fwd.push(i)
    back.pop
  end

  responder.value
end

puts <<~NOTE
  NOTE: Each round trip is: main push → Ractor pop → Ractor push → main pop.
        Latency is dominated by OS thread scheduling and wake-up time,
        not the queue operations themselves.
NOTE

# ─────────────────────────────────────────────────────────────
# 4. Worker Pool Throughput
#    One shared job queue, N Ractor workers, one shared result queue.
#    Models the most common real-world RactorQueue pattern.
# ─────────────────────────────────────────────────────────────

N_WORKERS = [Etc.nprocessors - 1, 1].max

section "4. Worker Pool Throughput (#{ITEMS} jobs, #{N_WORKERS} Ractor workers)"

bench("#{N_WORKERS}-Ractor pool (job queue + result queue)") do
  # Both queues sized to hold all items so neither producer ever blocks.
  # Small bounded queues + two chained queues = classic deadlock:
  #   main blocks pushing jobs → workers block pushing results → main
  #   can't drain results → deadlock.
  jobs    = RactorQueue.new(capacity: ITEMS + N_WORKERS)
  results = RactorQueue.new(capacity: ITEMS)

  workers = N_WORKERS.times.map do
    Ractor.new(jobs, results) do |jq, rq|
      loop do
        job = jq.pop(timeout: 60)
        break if job == :stop
        rq.push(job)  # minimal work: pass the value through
      end
    end
  end

  ITEMS.times     { |i| jobs.push(i) }
  N_WORKERS.times { jobs.push(:stop) }

  ITEMS.times { results.pop }
  workers.each(&:value)
end

puts <<~NOTE
  NOTE: Workers do minimal work (identity transform) so the result measures
        queue throughput, not compute. In real workloads, heavier compute
        per job reduces contention and improves per-item throughput.
NOTE

puts "\n#{SEPARATOR}"
puts "  Done."
puts "  System   : #{RUBY_DESCRIPTION}"
puts "  CPU cores: #{Etc.nprocessors}"
puts SEPARATOR
