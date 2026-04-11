# Internals

How `RactorQueue` is built — useful if you're contributing, debugging, or just curious.

---

## C++ Foundation: `atomic_queue`

The queue is backed by [`max0x7ba/atomic_queue`](https://github.com/max0x7ba/atomic_queue) — a C++14 header-only library that implements cache-line-aligned, lock-free MPMC queues using atomic compare-and-swap (CAS) operations.

`RactorQueue` uses `AtomicQueueB2<VALUE>`, the dynamically-sized variant. `VALUE` is MRI Ruby's opaque pointer type for Ruby objects — every Ruby object is represented as a `VALUE` (a `uintptr_t`).

The `AtomicQueueB2` template keeps two separate cache-line-aligned arrays for head/tail indices and for the ring buffer, minimizing false sharing between producers and consumers.

---

## Rice 4.x Bindings

The C++ class is exposed to Ruby via [Rice](https://github.com/jasonroelofs/rice) 4.x:

```cpp
Data_Type<StandardQueue> rb_cRQ = define_class<StandardQueue>("RactorQueue")
  .define_constructor(Constructor<StandardQueue, unsigned>())
  .define_method("c_try_push", &StandardQueue::try_push,
                 Arg("v").setValue())
  .define_method("c_try_pop",  &StandardQueue::try_pop,
                 Return().setValue())
  .define_method("capacity",   &StandardQueue::capacity)
  .define_method("was_size",   &StandardQueue::was_size)
  .define_method("was_empty",  &StandardQueue::was_empty)
  .define_method("was_full",   &StandardQueue::was_full);
```

Key details:

- `Arg("v").setValue()` — tells Rice to pass the argument as a raw `VALUE` (no type conversion); the C++ method receives the Ruby object pointer directly
- `Return().setValue()` — tells Rice to return the raw `VALUE` without conversion
- Without `.setValue()`, Rice would attempt to convert `VALUE` using its type-mapping system, which would incorrectly treat `VALUE` as an integer

---

## EMPTY_SENTINEL

The C extension needs a way to signal "the queue is empty" from `c_try_pop` without conflating it with a legitimately pushed `nil` (Ruby's `Qnil`, which is a valid `VALUE`).

The solution: a permanent, unique, frozen `Object` instance registered as a GC root:

```cpp
g_empty_sentinel = rb_obj_alloc(rb_cObject);
rb_obj_freeze(g_empty_sentinel);
rb_gc_register_mark_object(g_empty_sentinel);   // pin — never GC'd
rb_define_const(rb_cRQ, "EMPTY_SENTINEL", g_empty_sentinel);
```

`c_try_pop` returns this sentinel when the queue is empty. The Ruby-level `try_pop` checks for it with `result.equal?(RactorQueue::EMPTY_SENTINEL)` (identity comparison, not `==`), then converts it to `nil` for the caller.

The blocking `blocking_pop` checks for the sentinel in its spin loop: if it gets the sentinel, the queue is empty and it retries; any other value (including `nil`) is returned as-is.

---

## Ractor Shareability

Two things are needed to make a Rice-wrapped object `Ractor.shareable?`:

**1. `RUBY_TYPED_FROZEN_SHAREABLE` flag**

Rice sets `RUBY_TYPED_FREE_IMMEDIATELY` on the `rb_data_type_t` it creates. We OR-in `RUBY_TYPED_FROZEN_SHAREABLE` so that `Ractor.make_shareable(instance)` succeeds when the Ruby wrapper is frozen:

```cpp
Data_Type<StandardQueue>::ruby_data_type()->flags |= RUBY_TYPED_FROZEN_SHAREABLE;
```

**2. `rb_ext_ractor_safe(true)`**

This marks all methods registered by subsequent `rb_define_method` calls (which Rice uses internally) as safe to call from within a Ractor. Without this, Ruby raises an error when a Ractor attempts to call the C method:

```cpp
rb_ext_ractor_safe(true);
// ... define_class and define_method calls ...
rb_ext_ractor_safe(false);
```

**3. `Ractor.make_shareable` in the Ruby factory**

The Ruby-level `RactorQueue.new` calls `Ractor.make_shareable(instance)` after setting instance variables. This deep-freezes the Ruby wrapper object. The C++ `AtomicQueueB2` buffer is not a Ruby object and is unaffected by Ruby's freeze.

---

## File Layout

```
ractor_queue/
├── ext/
│   └── ractor_queue/
│       ├── extconf.rb          # mkmf config; finds Rice headers
│       ├── ractor_queue.cpp    # Rice bindings + Init_ractor_queue
│       └── standard_queue.h   # C++ wrapper around AtomicQueueB2<VALUE>
├── lib/
│   └── ractor_queue/
│       ├── ractor_queue.rb     # RactorQueue.new factory (capacity + make_shareable)
│       ├── interface.rb        # Ruby-level spin loops, try_push/pop, validate_shareable
│       ├── errors.rb           # Error, TimeoutError, NotShareableError
│       └── version.rb          # VERSION constant
├── vendor/
│   └── atomic_queue/
│       └── include/
│           └── atomic_queue/
│               └── atomic_queue.h  # vendored header (MIT)
└── test/
    ├── test_helper.rb
    ├── test_ractor_queue.rb    # Core API tests
    ├── test_ractor_safety.rb   # Ractor shareability and cross-Ractor round-trips
    └── test_timeout.rb         # Timeout behavior
```

---

## Ruby-Level Spin Loop

The blocking operations (`push`, `pop`) live entirely in Ruby (`lib/ractor_queue/interface.rb`). The C extension provides only the non-blocking `c_try_push` / `c_try_pop` primitives.

This keeps the GVL release/acquire cycle outside the hot path: the C methods are brief atomic operations that don't need to release the GVL, and the Ruby spin loop has access to full Ruby interrupt semantics (`Thread.pass`, `sleep`, `Thread#raise`).

```ruby
SPIN_THRESHOLD = 16
SLEEP_INTERVAL = 0.0001  # 100 µs

def blocking_push(obj, timeout)
  deadline = timeout ? Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout : nil
  spins = 0
  loop do
    return self if c_try_push(obj)
    raise TimeoutError if deadline && Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
    if spins < SPIN_THRESHOLD
      spins += 1
      Thread.pass
    else
      sleep(SLEEP_INTERVAL)
    end
  end
end
```
