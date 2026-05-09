# RactorQueue

A lock-free, bounded, MPMC queue that can be shared across Ruby Ractors.

## Why RactorQueue?

Ruby's built-in `Queue` uses a `Mutex` internally. Mutexes are not Ractor-shareable — attempting to pass a `Queue` to a Ractor raises `Ractor::IsolationError`:

```ruby
q = Queue.new
Ractor.shareable?(q)  # => false

# This raises Ractor::IsolationError:
Ractor.new(q) { |queue| queue.push(1) }
```

`RactorQueue` is lock-free. It has no mutex, no condition variable, no Ruby-level synchronization object. The queue itself is **always** `Ractor.shareable?` and can be handed to any number of Ractors simultaneously:

```ruby
q = RactorQueue.new(capacity: 1024)
Ractor.shareable?(q)  # => true

producer = Ractor.new(q) { |queue| 1000.times { |i| queue.push(i) } }
consumer = Ractor.new(q) { |queue| 1000.times { queue.pop } }

producer.value
consumer.value
```

## What It Is

- **Lock-free**: no mutex, no condition variable — operations use atomic compare-and-swap instructions directly
- **Bounded**: a fixed power-of-two capacity is set at construction time; `push` blocks when full, `pop` blocks when empty
- **MPMC**: multiple producers and multiple consumers can share a single queue safely
- **Ractor-safe**: the queue instance is always `Ractor.shareable?`; pass it directly to Ractor constructors

## What It Is Not

- **Not a replacement for `Queue` in threaded code** — under MRI threads, Ruby's `Queue` is faster because the GVL makes lock-free atomics unnecessary; RactorQueue's advantage is exclusive to Ractor workloads
- **Not unbounded** — capacity is fixed at construction; size up-front or use the [queue pool pattern](patterns.md#3-queue-pool-high-ractor-counts)

## Implementation

RactorQueue wraps [`max0x7ba/atomic_queue`](https://github.com/max0x7ba/atomic_queue) — a C++17 header-only, cache-line-aligned, lock-free MPMC queue — via [Rice](https://github.com/jasonroelofs/rice) 4.x bindings. The C++ buffer is unaffected by Ruby's freeze; only the Ruby wrapper object is frozen to satisfy `Ractor.make_shareable`.

## Next Steps

- [Installation](installation.md)
- [Quick Start](quick-start.md)
- [API Reference](api.md)
- [Ractor Patterns](patterns.md)
- [Concurrency Notes](concurrency.md)
- [Performance](performance.md)
