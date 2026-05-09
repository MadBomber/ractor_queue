# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Gem Does

RactorQueue is a lock-free, bounded, MPMC (Multi-Producer Multi-Consumer) queue for Ruby's Ractor concurrency model. Unlike Ruby's built-in `Queue` (which uses Mutex internally), RactorQueue is always `Ractor.shareable?` and can be safely passed to any number of Ractors simultaneously.

## Commands

```bash
# Install dependencies
bundle install

# Compile native C++ extension (required before running tests)
bundle exec rake compile

# Run all tests (default rake task)
bundle exec rake test

# Run a single test file
bundle exec ruby -Ilib:test test/test_ractor_queue.rb

# Run a single test by name
bundle exec ruby -Ilib:test test/test_ractor_queue.rb -n test_method_name

# Interactive console with gem loaded
bin/console
```

## Architecture

This gem has a three-layer architecture:

**1. C++ Extension** (`ext/ractor_queue/`)
- `standard_queue.h`: C++ `StandardQueue` class wrapping `atomic_queue::AtomicQueueB2<VALUE>` from the vendored header-only library at `vendor/atomic_queue/include/`
- `ractor_queue.cpp`: Rice 4.x bindings that expose C++ methods to Ruby, mark the extension as `RUBY_TYPED_FROZEN_SHAREABLE`, and define `EMPTY_SENTINEL` as a frozen GC-rooted Ruby object
- `extconf.rb`: Uses `mkmf-rice`; requires C++17 (`-std=c++17`); locates vendored atomic_queue headers

**2. Ruby Interface Layer** (`lib/ractor_queue/`)
- `interface.rb`: Implements blocking `push`/`pop` with exponential backoff spin loop — first 16 retries use `Thread.pass`, then `sleep(0.0001)` (100µs). This allows `Ctrl-C` and `Thread#raise` to interrupt.
- `errors.rb`: `TimeoutError`, `NotShareableError`
- `ractor_queue.rb`: `.new` factory that calls `Ractor.make_shareable` to freeze the queue instance

**3. Entry Point** (`lib/ractor_queue.rb`): Requires the native `.so`/`.dylib` then the Ruby modules.

## Key Implementation Details

**EMPTY Sentinel**: `try_pop` returns `RactorQueue::EMPTY_SENTINEL` (aliased as `RactorQueue::EMPTY`) when the queue is empty, and returns `nil` when `nil` was pushed. Always use `.equal?` (identity check), never `==`, to test for EMPTY.

**Ractor Shareability**: The queue is frozen via `Ractor.make_shareable` in `.new`. The C extension marks the type `RUBY_TYPED_FROZEN_SHAREABLE` so freezing succeeds on a mutable native object.

**Capacity**: Rounded up to the nearest power of two by `AtomicQueueB2`. The `capacity` method returns the actual (rounded) capacity.

**validate_shareable**: Optional `RactorQueue.new(capacity: N, validate_shareable: true)` checks `Ractor.shareable?(obj)` before each push; raises `NotShareableError` if not shareable.

## Test Files

- `test/test_ractor_queue.rb` — Core API: `try_push`, `try_pop`, size, empty/full, nil payloads
- `test/test_ractor_safety.rb` — Ractor shareable-ness and cross-Ractor access
- `test/test_timeout.rb` — Blocking push/pop with timeout and `TimeoutError`
- `test/test_async.rb` — Fiber-scheduler-aware `async_push`/`async_pop`: return values, timeout, cooperative fiber interleaving, MPMC with Async tasks
- `test/test_mpmc.rb` — MPMC correctness under Threads and Ractors: no loss, no duplication, asymmetric producer/consumer counts
- `test/test_helper.rb` — Shared helpers: `fill_queue`, `drain_queue`

Tests access C-layer methods directly (e.g., `c_try_push`, `c_try_pop`, `was_size`) since the Ruby interface wraps them.

## Build Requirements

- Ruby 3.2+ (MRI only — Ractor API)
- C++17-compatible compiler (Clang on macOS)
- `rake-compiler` and `rice ~> 4.0` gems (installed via Bundler)
- The `atomic_queue` library is vendored — no external C++ dependencies
