# Performance

## Benchmark Results

Measured on Apple M2 Max (12 cores, arm64), Ruby 4.0.2. Each benchmark transfers 50,000 items.

### 1P1C Throughput

| Configuration | Throughput | Latency |
|---|---|---|
| 1 producer Ractor / 1 consumer Ractor | ~470K ops/s | ~2,100 ns/op |

This is the baseline: two Ractors exchanging items through a single shared queue. Latency is dominated by OS thread scheduling — waking a sleeping Ractor takes ~100–200 µs under light load.

### Shared-Queue MPMC Scaling

All producers and consumers fight over a single queue (capacity 4096):

| Configuration | Throughput | Notes |
|---|---|---|
| 1P / 1C (2 Ractors) | ~470K ops/s | Baseline |
| 2P / 2C (4 Ractors) | ~855K ops/s | Near-linear |
| 4P / 4C (8 Ractors) | ~1.25M ops/s | Good scaling |
| 8P / 8C (16 Ractors) | ~1.53M ops/s | Diminishing returns |

Throughput increases with Ractor count but not linearly — each atomic CAS on the head/tail pointer invalidates that cache line on every other core. The exponential backoff (Phase 2 sleep) prevents scheduler thrashing, so 8P/8C completes without stalling.

### Queue-Pool MPMC Scaling

Each producer/consumer pair has its own queue, sized to hold all of that producer's items (producers never block):

| Configuration | Throughput | Notes |
|---|---|---|
| 1P / 1C (1 queue) | ~470K ops/s | Same as shared |
| 2P / 2C (2 queues) | ~940K ops/s | Linear scaling |
| 4P / 4C (4 queues) | ~1.37M ops/s | Near-linear |
| 8P / 8C (8 queues) | ~1.66M ops/s | Best throughput |

No cross-pair cache-line contention means scaling tracks core count more closely than the shared-queue approach.

### Ping-Pong Latency

Two Ractors exchange a single item at a time via two queues (5,000 round trips):

| Metric | Value |
|---|---|
| Round-trip latency | ~40,000 ns (40 µs) |
| Throughput | ~25K round trips/s |

Each round trip is: main push → Ractor pop → Ractor push → main pop. The ~40 µs is dominated by OS thread wake-up time, not the queue operations themselves.

### Worker Pool Throughput

One shared job queue, N Ractor workers, one shared result queue. Workers do minimal computation (identity transform):

| Workers | Throughput |
|---|---|
| 11 workers (M2 Max, 12 cores - 1) | ~1.2M ops/s |

With heavier per-job computation, per-item throughput can increase because less time is spent on queue contention relative to useful work.

---

## Comparison with Ruby's `Queue`

Ruby's built-in `Queue` is **not** included in these benchmarks — it cannot be shared across Ractors and has no equivalent role.

Under MRI **threads** (no Ractors), Ruby's `Queue` is faster than `RactorQueue` because the GVL makes lock-free atomics unnecessary. The GVL serializes Ruby code execution, so a simple mutex-based queue is lower overhead than atomic CAS loops. Use `RactorQueue` only when you need Ractor-based parallelism.

---

## Scaling Guidance

| Goal | Approach |
|---|---|
| Max throughput, few Ractors (≤ 2× cores) | Single shared queue |
| Max throughput, many Ractors (> 2× cores) | Queue pool |
| Minimize latency | Smaller queues (faster backoff turnaround), fewer Ractors |
| Dynamic load balancing | Small pool of shared queues (e.g., 4 for 16 Ractors) |

## Running the Benchmarks

```sh
bundle exec ruby examples/02_performance.rb
```

The benchmark script includes all four scenarios: 1P1C, shared MPMC, queue pool, and ping-pong latency.
