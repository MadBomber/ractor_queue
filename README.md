# ractor_queue

A lock-free, bounded, MPMC queue that can be shared across Ruby Ractors.

Ruby's built-in `Queue` uses a `Mutex` internally and cannot be passed as a shared reference across Ractor boundaries. `RactorQueue` has no mutex — it is always `Ractor.shareable?` and can be handed to any number of Ractors simultaneously.

```ruby
q = RactorQueue.new(capacity: 1024)

producer = Ractor.new(q) { |queue| 1000.times { |i| queue.push(i) } }
consumer = Ractor.new(q) { |queue| 1000.times { queue.pop } }

producer.value
consumer.value
```

Backed by the [max0x7ba/atomic_queue](https://github.com/max0x7ba/atomic_queue) C++14 header-only library via [Rice](https://github.com/jasonroelofs/rice) 4.x bindings.

---

## Installation

Add to your `Gemfile`:

```ruby
gem "ractor_queue"
```

Or install directly:

```sh
gem install ractor_queue
```

Requires MRI Ruby 3.2+ and a C++17 compiler. The native extension is built automatically on `gem install`.

---

## Quick Start

```ruby
require "ractor_queue"

# Create a bounded queue (capacity rounds up to the next power of two, minimum 4096)
q = RactorQueue.new(capacity: 256)

# Non-blocking
q.try_push(42)      # => true  (enqueued)
q.try_push(:hello)  # => true
q.try_pop           # => 42
q.try_pop           # => :hello
q.try_pop           # => RactorQueue::EMPTY  (queue was empty)

# Check for empty with identity comparison (never use ==)
v = q.try_pop
process(v) unless v.equal?(RactorQueue::EMPTY)

# Blocking — spin-waits until space / item is available
q.push(99)          # => self  (chainable)
q.pop               # => 99

# Blocking with timeout
q.pop(timeout: 0.5) # raises RactorQueue::TimeoutError after 500 ms if still empty

# Fiber-scheduler-aware — use inside Async { } blocks
require "async"
Async do
  q.async_push(42)            # => self  (yields via sleep(0) while full)
  q.async_pop                 # => 42   (yields via sleep(0) while empty)
  q.async_pop(timeout: 1.0)   # raises RactorQueue::TimeoutError after 1 s
end

# State (approximate under concurrency)
q.size              # => Integer
q.empty?            # => true / false
q.full?             # => true / false
q.capacity          # => Integer (exact)

# Always true — the queue itself is Ractor-shareable
Ractor.shareable?(q) # => true
```

---

## API

| Method | Returns | Notes |
|---|---|---|
| `RactorQueue.new(capacity:, validate_shareable: false)` | `RactorQueue` instance | Capacity rounded up to power-of-two minimum |
| `try_push(obj)` | `true` / `false` | Non-blocking; `false` if full |
| `try_pop` | `obj` or `RactorQueue::EMPTY` | Non-blocking; `EMPTY` sentinel if queue was empty; `nil` if `nil` was pushed |
| `push(obj, timeout: nil)` | `self` | Blocking; OS-thread backoff (sleep → suspend); best for Ractors and plain Threads |
| `pop(timeout: nil)` | `obj` | Blocking; OS-thread backoff; best for Ractors and plain Threads |
| `async_push(obj, timeout: nil)` | `self` | Fiber-scheduler-aware blocking push; yields via `sleep(0)`; best inside `Async { }` blocks |
| `async_pop(timeout: nil)` | `obj` | Fiber-scheduler-aware blocking pop; yields via `sleep(0)`; best inside `Async { }` blocks |
| `size` | Integer | Approximate element count |
| `empty?` | Boolean | Approximate |
| `full?` | Boolean | Approximate |
| `capacity` | Integer | Exact allocated capacity |

### Errors

| Class / Constant | Meaning |
|---|---|
| `RactorQueue::EMPTY` | Sentinel returned by `try_pop` when the queue is empty. Check with `equal?`, never `==`. |
| `RactorQueue::TimeoutError` | Raised by `push` or `pop` when the `timeout:` deadline expires. |
| `RactorQueue::NotShareableError` | Raised by `push`/`try_push` when `validate_shareable: true` and the object is not Ractor-shareable. |

### `validate_shareable`

With `validate_shareable: true`, the queue raises `NotShareableError` at push time for any non-shareable object, catching mistakes before they reach a Ractor boundary:

```ruby
safe_q = RactorQueue.new(capacity: 64, validate_shareable: true)

safe_q.push(42)             # ok — Integer is shareable
safe_q.push("hello".freeze) # ok — frozen String is shareable
safe_q.push([1, 2, 3])      # raises RactorQueue::NotShareableError
```

---

## Ractor Patterns

### 1. Single Producer / Single Consumer (1P1C)

The baseline pattern — one Ractor feeds another through a shared queue.

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
puts consumer.value.inspect
```

### 2. Worker Pool (MPMC)

A shared job queue drained by N Ractor workers. Size the queues large enough to hold all in-flight items — chaining two small bounded queues risks deadlock (see [Concurrency Notes](#concurrency-notes)).

```ruby
WORKERS = 8
jobs    = RactorQueue.new(capacity: 10_000)
results = RactorQueue.new(capacity: 10_000)

workers = WORKERS.times.map do
  Ractor.new(jobs, results) do |jq, rq|
    loop do
      job = jq.pop(timeout: 30)
      break if job == :stop
      rq.push(job * job)        # do work
    end
  end
end

1000.times { |i| jobs.push(i) }
WORKERS.times { jobs.push(:stop) }

results_list = 1000.times.map { results.pop }
workers.each(&:value)
```

### 3. Queue Pool (High Ractor Counts)

When many Ractors share a single bounded queue, the spin-wait backoff keeps things moving, but beyond ~2× core count you get diminishing returns from cache-line contention. Use one queue per producer/consumer pair — zero cross-pair contention, linear scaling to core count:

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

---

## Concurrency Notes

### Spin-wait backoff

`push` and `pop` use an exponential backoff spin loop: the first 16 retries call `Thread.pass`; subsequent retries call `sleep(0.0001)`. The sleep actually suspends the OS thread, preventing scheduler thrashing when many Ractors are blocked on the same queue.

This means `Thread#raise` and `Ctrl-C` can interrupt a blocked `push` or `pop` at any point.

### Two-queue deadlock

Chaining two bounded queues in a pipeline can deadlock when both queues are full simultaneously:

```
main blocks pushing to jobs (full)
  ↓
workers block pushing to results (full)
  ↓
main cannot drain results (it is blocked)
  ↓  deadlock
```

**Fix:** size at least one queue large enough that its producer never blocks, or drain results asynchronously in a separate Ractor.

### Spin-wait storm

When more Ractors are actively spinning (blocked on `push`/`pop`) than there are idle cores, the OS scheduler can thrash. The sleep-based backoff mitigates this, but the practical ceiling for a single shared queue is roughly `2 × CPU cores` Ractors doing nothing but queue operations. For higher Ractor counts, use the queue pool pattern.

### `nil` as a payload

`try_pop` returns `RactorQueue::EMPTY` when the queue is empty and `nil` when `nil` was the pushed value — the two are unambiguous. Always check for empty with identity comparison:

```ruby
v = q.try_pop
return if v.equal?(RactorQueue::EMPTY)
process(v)   # v may be nil — that's fine, it's a real payload
```

Do not use `==` to check for `EMPTY` — use `equal?`.

---

## Performance

Measured on Apple M2 Max (12 cores), Ruby 4.0.2:

| Configuration | Throughput |
|---|---|
| 1 producer / 1 consumer Ractor | ~470K ops/s |
| 2P / 2C shared queue | ~855K ops/s |
| 4P / 4C shared queue | ~1.25M ops/s |
| 8P / 8C shared queue | ~1.53M ops/s |
| 8P / 8C queue pool (8 queues) | ~1.66M ops/s |
| 50P / 50C queue pool | ~1.60M ops/s |

Ruby's built-in `Queue` is not included — it cannot participate in Ractor benchmarks.

Under MRI threads (no Ractors), Ruby's `Queue` is faster because the GVL makes lock-free atomics unnecessary. RactorQueue's advantage is exclusive to Ractor workloads.

---

## Running the Examples

```sh
bundle exec ruby examples/01_basic_usage.rb   # Ractor usage patterns
bundle exec ruby examples/02_performance.rb   # Throughput benchmarks
```

---

## Development

```sh
bundle install
bundle exec rake compile   # build the native extension
bundle exec rake test      # run the test suite
```

---

## Documentation

| Document | Description |
|---|---|
| [`examples/01_basic_usage.rb`](examples/01_basic_usage.rb) | Annotated Ractor usage patterns (1P1C, timeout, worker pool, pipeline, validate_shareable) |
| [`examples/02_performance.rb`](examples/02_performance.rb) | Throughput benchmarks across queue topologies and Ractor counts |
| [`docs/superpowers/specs/2026-04-10-atomic-queue-design.md`](docs/superpowers/specs/2026-04-10-atomic-queue-design.md) | Original design specification (C extension architecture, Rice bindings, API design decisions) |
| [`docs/superpowers/plans/`](docs/superpowers/plans/) | Implementation plans for each development phase |

---

## License

MIT. The vendored [max0x7ba/atomic_queue](https://github.com/max0x7ba/atomic_queue) C++ library is also MIT licensed.
