# API Reference

## Constructor

### `RactorQueue.new(capacity:, validate_shareable: false)`

Creates a new bounded MPMC queue.

**Parameters:**

| Name | Type | Required | Default | Description |
|---|---|---|---|---|
| `capacity` | Integer | yes | — | Maximum element count. Rounded up to next power of two; minimum 4096 on arm64. |
| `validate_shareable` | Boolean | no | `false` | When `true`, raises `NotShareableError` on `push`/`try_push` if the object fails `Ractor.shareable?`. |

**Returns:** A new `RactorQueue` instance that is always `Ractor.shareable?`.

```ruby
q = RactorQueue.new(capacity: 1024)
q = RactorQueue.new(capacity: 64, validate_shareable: true)
```

---

## Instance Methods

### `try_push(obj) → true / false`

Attempts to enqueue `obj` without blocking.

- Returns `true` if the object was enqueued.
- Returns `false` if the queue is full.
- If `validate_shareable: true` was set at construction, raises `NotShareableError` for non-shareable objects before attempting the push.

```ruby
q.try_push(42)      # => true
q.try_push(:stop)   # => true
# (when full:)
q.try_push("more")  # => false
```

---

### `try_pop → obj or RactorQueue::EMPTY`

Attempts to dequeue the next element without blocking.

- Returns the dequeued object if one was available (including `nil` if `nil` was pushed).
- Returns `RactorQueue::EMPTY` — a unique frozen sentinel — if the queue is empty.

Check for empty with identity comparison:

```ruby
v = q.try_pop
if v.equal?(RactorQueue::EMPTY)
  # queue was empty
else
  process(v)   # v may be nil — that's a real payload, not empty
end
```

`nil` and `EMPTY` are always distinguishable:

```ruby
q.push(nil)
q.try_pop           # => nil               (the nil we pushed)
q.try_pop           # => RactorQueue::EMPTY (queue is now empty)
```

!!! warning "Use `equal?`, not `==`"
    `EMPTY` is identified by object identity. Never compare with `==` — use `equal?`.

---

### `push(obj, timeout: nil) → self`

Enqueues `obj`, blocking until space is available.

- Returns `self` (chainable).
- Raises `TimeoutError` if `timeout:` is given and the deadline expires before space becomes available.
- `timeout: 0` tries exactly once and immediately raises if the queue is full.
- If `validate_shareable: true`, raises `NotShareableError` before blocking if the object is not shareable.

The blocking spin loop uses exponential backoff: the first 16 retries call `Thread.pass`; subsequent retries call `sleep(0.0001)` (100 µs). This makes `Thread#raise` and `Ctrl-C` effective at interrupting a blocked push.

```ruby
q.push(42)                  # blocks until space
q.push(42, timeout: 1.0)    # raises TimeoutError after 1 second
q.push(42).push(43)         # chainable
```

---

### `pop(timeout: nil) → obj`

Dequeues the next element, blocking until one is available.

- Returns the dequeued object (including `nil` if `nil` was pushed).
- Raises `TimeoutError` if `timeout:` is given and the deadline expires before an element becomes available.
- `timeout: 0` tries exactly once and immediately raises if the queue is empty.

```ruby
q.pop                       # blocks until an item is available
q.pop(timeout: 0.5)         # raises TimeoutError after 500 ms
```

---

### `size → Integer`

Returns the approximate number of elements currently in the queue.

Approximate: may be momentarily stale under concurrent pushes/pops.

```ruby
q.push(1).push(2).push(3)
q.size  # => 3 (approximately)
```

---

### `empty? → Boolean`

Returns `true` if the queue appears empty.

Approximate under concurrency — the queue may have changed by the time you act on the result.

```ruby
q.empty?  # => true / false
```

---

### `full? → Boolean`

Returns `true` if the queue appears full.

Approximate under concurrency.

```ruby
q.full?  # => true / false
```

---

### `capacity → Integer`

Returns the exact allocated capacity of the queue. This is the value rounded up to the next power of two at construction time.

```ruby
RactorQueue.new(capacity: 100).capacity  # => 4096 (minimum on arm64)
RactorQueue.new(capacity: 5000).capacity # => 8192
```

---

## Error Classes

### `RactorQueue::Error`

Base class for all `RactorQueue` errors. Inherits from `StandardError`.

### `RactorQueue::TimeoutError`

Raised by `push` or `pop` when the `timeout:` deadline expires before the operation could complete.

```ruby
begin
  q.pop(timeout: 0.1)
rescue RactorQueue::TimeoutError
  puts "nothing arrived in time"
end
```

### `RactorQueue::NotShareableError`

Raised by `push` or `try_push` when `validate_shareable: true` and the object fails `Ractor.shareable?`.

```ruby
safe_q = RactorQueue.new(capacity: 8, validate_shareable: true)

begin
  safe_q.push([1, 2, 3])
rescue RactorQueue::NotShareableError => e
  puts e.message  # "[1, 2, 3] is not Ractor-shareable"
end
```

---

## Constants

### `RactorQueue::EMPTY`

A permanently frozen `Object` instance returned by `try_pop` when the queue is empty. It is a GC root (never collected), always the same object identity, and always `Ractor.shareable?`.

```ruby
v = q.try_pop
return if v.equal?(RactorQueue::EMPTY)
# v is a real value — including nil if nil was pushed
```

!!! note
    `RactorQueue::EMPTY_SENTINEL` is an internal alias for the same object. Application code should use `RactorQueue::EMPTY`.

Do not push `EMPTY` into a queue — it is a sentinel, not a payload.

---

## Thread Safety

All public methods are safe to call concurrently from any number of Ruby threads or Ractors. There are no locks; operations use atomic compare-and-swap instructions at the C++ level.

State queries (`size`, `empty?`, `full?`) read a snapshot that may be stale by the time the call returns — this is inherent to lock-free data structures and is the same behavior as Java's `ConcurrentLinkedQueue.size()`.
