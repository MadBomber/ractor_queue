# Concurrency Notes

---

## Spin-Wait Backoff

`push` and `pop` use a two-phase exponential backoff spin loop:

**Phase 1 — `Thread.pass` (retries 0–15):**

The first 16 retries call `Thread.pass`, which invokes `sched_yield()` at the OS level. This is cheap and yields to another thread at the same scheduler priority. It's fast when the queue clears quickly (light contention, brief full/empty states).

`Thread.pass` also triggers Ruby's interrupt-checking machinery, so `Thread#raise` and `Ctrl-C` can escape a blocked push or pop during this phase.

**Phase 2 — `sleep(0.0001)` (retries 16+):**

After 16 `Thread.pass` calls, subsequent retries sleep for 100 µs. `sleep()` actually suspends the OS thread (rather than spin-yielding it), freeing the core for consumers or other Ractors to make real progress.

This sleep phase is critical for Ractor workloads: each Ractor is its own OS thread. Under high contention, if all Ractors call `sched_yield()` in a tight loop, the OS rotates them at the same priority level without allowing any to advance. `sleep` breaks this cycle.

```
retry 0–15:   Thread.pass   → fast path, low latency
retry 16+:    sleep(0.0001) → OS thread yields core, prevents scheduler storm
```

---

## Two-Queue Deadlock

Chaining two bounded queues in a pipeline with small capacities can deadlock:

```
main blocks pushing to jobs (full)
  ↓
workers block pushing to results (full)
  ↓
main cannot drain results (it is blocked on jobs)
  ↓  deadlock
```

**Example of the broken pattern:**

```ruby
# DANGEROUS — both queues are small; deadlock is possible
jobs    = RactorQueue.new(capacity: 64)
results = RactorQueue.new(capacity: 64)

# If main fills `jobs` and workers fill `results` simultaneously,
# the whole system locks up.
```

**Fix 1: Size queues to hold all in-flight items**

```ruby
ITEMS   = 10_000
WORKERS = 8

jobs    = RactorQueue.new(capacity: ITEMS + WORKERS)  # never fills up
results = RactorQueue.new(capacity: ITEMS)             # never fills up

ITEMS.times   { |i| jobs.push(i) }    # non-blocking — queue is large enough
WORKERS.times { jobs.push(:stop) }
```

**Fix 2: Drain results asynchronously**

```ruby
jobs    = RactorQueue.new(capacity: 128)
results = RactorQueue.new(capacity: 128)

# A dedicated drain Ractor processes results while main keeps pushing jobs
drainer = Ractor.new(results) do |rq|
  all = []
  loop { v = rq.pop(timeout: 60); break if v == :stop; all << v }
  all
end

# Now main can push jobs freely — it never needs to read results
ITEMS.times   { |i| jobs.push(i) }
WORKERS.times { jobs.push(:stop) }
results.push(:stop)

drainer.value  # collect everything after all workers are done
```

---

## Spin-Wait Storm

When more Ractors are actively spinning (blocked on `push`/`pop`) than there are idle CPU cores, the OS scheduler can thrash. Each spinning Ractor calls `sched_yield()` in a tight loop, generating constant context-switch overhead without making progress.

The sleep-based backoff in Phase 2 mitigates this, but the practical ceiling for a **single shared queue** doing pure queue operations is roughly `2 × CPU cores` Ractors. Beyond that, use the [queue pool pattern](patterns.md#3-queue-pool-high-ractor-counts).

| Ractor count | Recommended approach |
|---|---|
| ≤ 2× cores | Single shared queue is fine |
| > 2× cores | Queue pool (one queue per producer/consumer pair) |
| Very high (50+) | Queue pool; pairs share work via chunked batching |

---

## `nil` as a Payload

`nil` is an unambiguous payload. `try_pop` returns `RactorQueue::EMPTY` (a unique frozen sentinel) when the queue is empty, and `nil` when `nil` was actually pushed:

```ruby
q = RactorQueue.new(capacity: 8)
q.push(nil)

q.try_pop   # => nil                (the nil we pushed)
q.try_pop   # => RactorQueue::EMPTY (queue is now empty — clearly distinct)
```

Always check for empty with identity comparison:

```ruby
v = q.try_pop
return if v.equal?(RactorQueue::EMPTY)
process(v)   # v may be nil — that's a real payload
```

The blocking `pop` has no empty case — it only returns when a value was actually dequeued.

---

## Approximate State Queries

`size`, `empty?`, and `full?` read a snapshot from the underlying C++ atomic counter. Under concurrent pushes and pops, the snapshot may be stale by the time the Ruby call returns. This is inherent to lock-free data structures.

**Do not use state queries for coordination logic.** For example, spinning on `empty?` to wait for an item is wrong — use `pop` instead. The correct use of state queries is for monitoring, logging, or sizing decisions at startup.

---

## Ruby 4.0 Ractor Semantics

In Ruby 4.0, non-shareable objects no longer raise `Ractor::IsolationError` when crossing Ractor boundaries via `Ractor#value`. RactorQueue's `validate_shareable: false` (default) allows pushing mutable objects without error — they can be consumed by the Ractor without isolation faults.

If you need strict enforcement that only shareable objects enter the queue, use `validate_shareable: true`.
