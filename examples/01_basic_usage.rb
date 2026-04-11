#!/usr/bin/env ruby
# frozen_string_literal: true

$stdout.sync = true

# examples/01_basic_usage.rb
#
# Demonstrates RactorQueue — the only bounded MPMC queue in Ruby
# that can be shared across Ractor boundaries.
#
# Ruby's built-in Queue uses Mutex internally, which is not
# Ractor-shareable. RactorQueue is lock-free and always shareable.
#
# Run from the project root:
#   bundle exec ruby examples/01_basic_usage.rb

$LOAD_PATH.unshift File.expand_path("../lib", __dir__)
require "ractor_queue"

SEPARATOR = "-" * 60

def section(title)
  puts "\n#{SEPARATOR}"
  puts "  #{title}"
  puts SEPARATOR
end

# ─────────────────────────────────────────────────────────────
# 1. Why RactorQueue?
# ─────────────────────────────────────────────────────────────

section "1. Why RactorQueue?"

puts "Queue       Ractor.shareable? => #{Ractor.shareable?(Queue.new)}"
puts "RactorQueue Ractor.shareable? => #{Ractor.shareable?(RactorQueue.new(capacity: 64))}"

puts <<~NOTE
  Ruby's Queue uses a Mutex internally — Mutex is not Ractor-shareable.
  Attempting to pass a Queue to a Ractor would raise Ractor::IsolationError.

  RactorQueue is lock-free and always Ractor-shareable. Pass the same
  RactorQueue instance to any number of Ractors as a shared reference.
NOTE

# ─────────────────────────────────────────────────────────────
# 2. Single Producer / Single Consumer (1P1C)
# ─────────────────────────────────────────────────────────────

section "2. Single Producer / Single Consumer (1P1C)"

q = RactorQueue.new(capacity: 32)

producer = Ractor.new(q) do |queue|
  5.times { |i| queue.push(i * i) }
  queue.push(:done)
end

consumer = Ractor.new(q) do |queue|
  results = []
  loop do
    value = queue.pop
    break if value == :done
    results << value
  end
  results
end

producer.value
puts "Squares from Ractor producer: #{consumer.value.inspect}"

# ─────────────────────────────────────────────────────────────
# 3. try_pop and RactorQueue::EMPTY
#
# try_push / try_pop never block. try_pop returns RactorQueue::EMPTY
# (a unique frozen sentinel) when the queue is empty, not nil.
# This makes nil an unambiguous payload value.
# ─────────────────────────────────────────────────────────────

section "3. try_pop and RactorQueue::EMPTY"

demo_q = RactorQueue.new(capacity: 8)

# --- non-blocking push ---
demo_q.try_push(42)
demo_q.try_push(:hello)
demo_q.try_push(nil)   # nil is a valid payload

# --- non-blocking pop ---
v1 = demo_q.try_pop    # => 42
v2 = demo_q.try_pop    # => :hello
v3 = demo_q.try_pop    # => nil   (the nil we pushed — not EMPTY)
v4 = demo_q.try_pop    # => RactorQueue::EMPTY (queue is now empty)

puts "try_pop results:"
puts "  pop 1 : #{v1.inspect}  empty?=#{v1.equal?(RactorQueue::EMPTY)}"
puts "  pop 2 : #{v2.inspect}  empty?=#{v2.equal?(RactorQueue::EMPTY)}"
puts "  pop 3 : #{v3.inspect}  empty?=#{v3.equal?(RactorQueue::EMPTY)}"
puts "  pop 4 : #{v4.inspect}  empty?=#{v4.equal?(RactorQueue::EMPTY)}"

puts <<~NOTE

  nil (pop 3) and EMPTY (pop 4) are distinct objects — nil payloads are
  never confused with an empty queue. Always check with equal?, not ==.

  Typical usage pattern:
    loop do
      v = q.try_pop
      break if v.equal?(RactorQueue::EMPTY)
      process(v)
    end
NOTE

# try_push returns false when the queue is full (never blocks)
full_q = RactorQueue.new(capacity: 4)
full_q.capacity.times { full_q.try_push(0) }  # fill to capacity
puts "try_push when full => #{full_q.try_push(99)}"  # => false

# ─────────────────────────────────────────────────────────────
# 4. pop with Timeout — Handle an Empty Queue Gracefully
# ─────────────────────────────────────────────────────────────

section "4. pop with Timeout"

# Ractor pops from an empty queue; times out after 50 ms
empty_q = RactorQueue.new(capacity: 8)
r1 = Ractor.new(empty_q) do |queue|
  queue.pop(timeout: 0.05)
rescue RactorQueue::TimeoutError
  :timed_out
end
puts "Empty queue  => #{r1.value.inspect}"

# Ractor pops an item that is already present
preloaded_q = RactorQueue.new(capacity: 8)
preloaded_q.push(:ready)
r2 = Ractor.new(preloaded_q) do |queue|
  queue.pop(timeout: 5)
end
puts "Loaded queue => #{r2.value.inspect}"

# ─────────────────────────────────────────────────────────────
# 5. Worker Pool — MPMC with Multiple Ractor Workers
# ─────────────────────────────────────────────────────────────

section "5. Worker Pool (MPMC)"

JOB_COUNT    = 20
WORKER_COUNT = 4

jobs    = RactorQueue.new(capacity: 64)
results = RactorQueue.new(capacity: 64)

# Enqueue all jobs before workers start (main Ractor pushes freely)
JOB_COUNT.times { |i| jobs.push(i) }
WORKER_COUNT.times { jobs.push(:stop) }  # one sentinel per worker

workers = WORKER_COUNT.times.map do
  Ractor.new(jobs, results) do |jq, rq|
    loop do
      job = jq.pop(timeout: 10)
      break if job == :stop
      rq.push(job * job)  # compute the square as the "job"
    end
  end
end

workers.each(&:value)

# Drain results with try_pop — EMPTY signals the queue is exhausted
squares = []
loop do
  v = results.try_pop
  break if v.equal?(RactorQueue::EMPTY)
  squares << v
end

puts "#{WORKER_COUNT} Ractor workers processed #{squares.size} jobs"
puts "First 5 results (sorted): #{squares.sort.first(5).inspect} ..."

# ─────────────────────────────────────────────────────────────
# 6. Two-Stage Pipeline — Chained Ractors
# ─────────────────────────────────────────────────────────────

section "6. Two-Stage Pipeline"

# Input values flow: main -> [Stage 1: ×2] -> [Stage 2: ×3] -> main
raw    = RactorQueue.new(capacity: 16)
middle = RactorQueue.new(capacity: 16)

stage1 = Ractor.new(raw, middle) do |src, dst|
  loop do
    v = src.pop(timeout: 5)
    break if v == :done
    dst.push(v * 2)
  end
  dst.push(:done)
end

stage2 = Ractor.new(middle) do |src|
  output = []
  loop do
    v = src.pop(timeout: 5)
    break if v == :done
    output << v * 3
  end
  output
end

5.times { |i| raw.push(i + 1) }  # push 1..5
raw.push(:done)

stage1.value
puts "Pipeline (×2, then ×3): #{stage2.value.inspect}"  # [6, 12, 18, 24, 30]

# ─────────────────────────────────────────────────────────────
# 7. validate_shareable — Enforce Ractor-safe Payloads
# ─────────────────────────────────────────────────────────────

section "7. validate_shareable — Enforce Ractor-safe Payloads"

# With validate_shareable: true the queue rejects non-shareable objects
# at push time, catching bad producers before a value reaches a Ractor.

safe_q = RactorQueue.new(capacity: 16, validate_shareable: true)

safe_q.push(42)
safe_q.push(:symbol)
safe_q.push("frozen".freeze)
puts "Pushed: Integer, Symbol, frozen String — all accepted"

begin
  safe_q.push([1, 2, 3])      # mutable Array — rejected
rescue RactorQueue::NotShareableError => e
  puts "Rejected: #{e.message}"
end

begin
  safe_q.push({ key: "value" })  # mutable Hash — rejected
rescue RactorQueue::NotShareableError => e
  puts "Rejected: #{e.message}"
end

# The guard fires inside a Ractor too — catches bad producers at the source
bad_producer = Ractor.new(safe_q) do |queue|
  queue.push([4, 5, 6])
rescue RactorQueue::NotShareableError => err
  "Ractor caught NotShareableError: #{err.message}"
end
puts bad_producer.value

puts "\n#{SEPARATOR}"
puts "  Done. All Ractor examples completed successfully."
puts SEPARATOR
