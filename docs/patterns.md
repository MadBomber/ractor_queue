# Ractor Patterns

Common patterns for using `RactorQueue` across Ractor boundaries.

---

## 1. Single Producer / Single Consumer (1P1C)

The baseline pattern — one Ractor feeds another through a shared queue. Use a sentinel value (`:done`, `:stop`, etc.) to signal end-of-stream.

```ruby
q = RactorQueue.new(capacity: 1024)

producer = Ractor.new(q) do |queue|
  100.times { |i| queue.push(i * i) }
  queue.push(:done)
end

consumer = Ractor.new(q) do |queue|
  results = []
  loop do
    v = queue.pop
    break if v == :done
    results << v
  end
  results
end

producer.value
puts consumer.value.inspect  # [0, 1, 4, 9, 16, ...]
```

**Queue sizing:** With a single producer and consumer, a modest queue (1024–4096) is sufficient. The producer can run ahead while the consumer processes, smoothing out bursts.

---

## 2. Worker Pool (MPMC)

A shared job queue drained by N Ractor workers. One sentinel per worker signals shutdown:

```ruby
WORKERS = 8
jobs    = RactorQueue.new(capacity: 10_000)
results = RactorQueue.new(capacity: 10_000)

workers = WORKERS.times.map do
  Ractor.new(jobs, results) do |jq, rq|
    loop do
      job = jq.pop(timeout: 30)
      break if job == :stop
      rq.push(job * job)  # do work
    end
  end
end

1000.times { |i| jobs.push(i) }
WORKERS.times { jobs.push(:stop) }  # one per worker

results_list = 1000.times.map { results.pop }
workers.each(&:value)
```

!!! warning "Two-Queue Deadlock"
    Chaining two **small** bounded queues can deadlock (see [Concurrency Notes](concurrency.md#two-queue-deadlock)).
    Size the queues large enough that producers never block while consumers are blocked on the other queue,
    or drain the result queue asynchronously in a separate Ractor.

**Queue sizing for the worker pool pattern:**

- `jobs` capacity: at least `job_count + worker_count` (so main can push all jobs without blocking)
- `results` capacity: at least `job_count` (so workers can push all results without blocking)

---

## 3. Queue Pool (High Ractor Counts)

When many Ractors share a single bounded queue, the spin-wait backoff keeps things moving, but beyond roughly `2 × CPU cores` Ractors doing pure queue operations, cache-line contention causes diminishing returns.

The queue pool pattern gives each producer/consumer pair its own queue — zero cross-pair contention, linear scaling to core count:

```ruby
PAIRS = 16   # 32 Ractors total

pairs = PAIRS.times.map do
  q = RactorQueue.new(capacity: 1024)
  p = Ractor.new(q) { |queue| 1000.times { |i| queue.push(i) } }
  c = Ractor.new(q) { |queue| 1000.times { queue.pop } }
  [p, c]
end

pairs.each { |p, c| p.value; c.value }
```

**Trade-off:** Work is statically partitioned — each producer feeds only its paired consumer. For dynamic load balancing across many workers, use a small number of shared queues (e.g., 4 queues for 16 Ractors) with jobs chunked large enough that producers rarely hit the capacity limit.

---

## 4. Two-Stage Pipeline

Ractors chained in stages, each transforming values and passing them downstream:

```ruby
raw    = RactorQueue.new(capacity: 64)
middle = RactorQueue.new(capacity: 64)

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
puts stage2.value.inspect  # [6, 12, 18, 24, 30]
```

!!! tip "Pipeline Queue Sizing"
    In a pipeline, each stage's output queue should be large enough to buffer a burst from the upstream stage.
    If stages process at different speeds, size the intermediate queues to the slower stage's burst capacity.

---

## 5. Validate-Shareable Guard

Use `validate_shareable: true` on queues that feed Ractors to catch bad producers at the push site rather than at the Ractor boundary:

```ruby
safe_q = RactorQueue.new(capacity: 64, validate_shareable: true)

safe_q.push(42)             # ok
safe_q.push("hello".freeze) # ok
safe_q.push(:symbol)        # ok

safe_q.push([1, 2, 3])      # raises RactorQueue::NotShareableError immediately
```

The guard also fires inside a Ractor block:

```ruby
bad_producer = Ractor.new(safe_q) do |queue|
  queue.push([4, 5, 6])
rescue RactorQueue::NotShareableError => err
  "caught: #{err.message}"
end

puts bad_producer.value  # "caught: [4, 5, 6] is not Ractor-shareable"
```

---

## Choosing a Pattern

| Situation | Pattern |
|---|---|
| Simple producer → consumer handoff | [1P1C](#1-single-producer-single-consumer-1p1c) |
| Dynamic job dispatch to N workers | [Worker Pool](#2-worker-pool-mpmc) |
| Ractor count > 2× CPU cores | [Queue Pool](#3-queue-pool-high-ractor-counts) |
| Multi-stage data transformation | [Pipeline](#4-two-stage-pipeline) |
| Need to enforce Ractor-safe payloads | [validate_shareable](#5-validate-shareable-guard) |
