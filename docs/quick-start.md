# Quick Start

```ruby
require "ractor_queue"

# Create a bounded queue (capacity rounds up to next power of two, minimum 4096)
q = RactorQueue.new(capacity: 256)
```

## Non-Blocking Operations

`try_push` and `try_pop` never block. They return immediately with a success indicator or the value:

```ruby
q.try_push(42)      # => true   (enqueued)
q.try_push(:hello)  # => true
q.try_pop           # => 42
q.try_pop           # => :hello
q.try_pop           # => RactorQueue::EMPTY  (queue is empty)

# nil is an unambiguous payload — EMPTY is a distinct sentinel
q.try_push(nil)     # => true
q.try_pop           # => nil                (the nil we pushed)
q.try_pop           # => RactorQueue::EMPTY (queue is now empty)
```

Check for empty with identity comparison (`equal?`, not `==`):

```ruby
v = q.try_pop
unless v.equal?(RactorQueue::EMPTY)
  process(v)   # v may be nil — that's a real payload
end
```

## Blocking Operations

`push` and `pop` spin-wait until space is available (push) or an item is available (pop):

```ruby
q.push(99)   # => self  (blocks until space; chainable)
q.pop        # => 99    (blocks until an item is present)
```

Blocking operations use an exponential backoff: first 16 retries call `Thread.pass`, then subsequent retries call `sleep(0.0001)`. This keeps latency low under light contention while preventing scheduler thrashing under heavy load.

## Timeouts

Both `push` and `pop` accept an optional `timeout:` (in seconds). If the deadline expires, `RactorQueue::TimeoutError` is raised:

```ruby
q.push(1, timeout: 0.5)  # raises TimeoutError after 500 ms if still full
q.pop(timeout: 0.5)      # raises TimeoutError after 500 ms if still empty

# timeout: 0 means "try exactly once, then raise"
q.pop(timeout: 0)        # raises TimeoutError immediately if empty
```

## State Queries

```ruby
q.size      # => Integer  (approximate element count)
q.empty?    # => true / false (approximate)
q.full?     # => true / false (approximate)
q.capacity  # => Integer  (exact allocated capacity)
```

State queries are approximate under concurrency — by the time you act on the result, it may have changed.

## Shareable Validation

With `validate_shareable: true`, the queue raises `NotShareableError` at push time for any non-shareable object, catching Ractor isolation mistakes before they propagate:

```ruby
safe_q = RactorQueue.new(capacity: 64, validate_shareable: true)

safe_q.push(42)             # ok — Integer is always shareable
safe_q.push("hello".freeze) # ok — frozen String is shareable
safe_q.push(:symbol)        # ok — Symbols are always shareable

safe_q.push([1, 2, 3])      # raises RactorQueue::NotShareableError
safe_q.push({ key: "val" }) # raises RactorQueue::NotShareableError
```

## Using with Ractors

The queue is always `Ractor.shareable?` — pass it directly to Ractor constructors:

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

See [Ractor Patterns](patterns.md) for 1P1C, worker pool, and queue pool examples.
