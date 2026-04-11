# Development

## Setup

```sh
git clone https://github.com/MadBomber/ractor_queue
cd ractor_queue
bundle install
bundle exec rake compile
```

`rake compile` builds the native C++ extension. You must run this after any change to files under `ext/`.

## Running Tests

```sh
bundle exec rake test
```

The test suite covers:

| File | What it tests |
|---|---|
| `test/test_ractor_queue.rb` | Core API: push, pop, try_push, try_pop, capacity, size, empty?, full?, validate_shareable |
| `test/test_ractor_safety.rb` | Ractor shareability, cross-Ractor round-trips |
| `test/test_timeout.rb` | Timeout behavior for push and pop |

## Running Examples

```sh
bundle exec ruby examples/01_basic_usage.rb   # Ractor usage patterns
bundle exec ruby examples/02_performance.rb   # Throughput benchmarks
```

Both files have `$stdout.sync = true` at the top so output is not buffered when run in the background or redirected.

## Adding a New Method

1. If the method requires C-level access (direct queue operations): add it to `ext/ractor_queue/standard_queue.h` and expose it in `ext/ractor_queue/ractor_queue.cpp`, then `bundle exec rake compile`

2. If the method is pure Ruby (built on `c_try_push`/`c_try_pop`): add it to `lib/ractor_queue/interface.rb`

3. Write a test in the appropriate `test/` file before implementing

## Editing the C Extension

The C extension entry point is `ext/ractor_queue/ractor_queue.cpp`. The queue wrapper is `ext/ractor_queue/standard_queue.h`.

After any change to `ext/`:

```sh
bundle exec rake compile
```

The compiled `.bundle` (macOS) or `.so` (Linux) is placed in `lib/ractor_queue/`.

## Vendored Library

`vendor/atomic_queue/include/atomic_queue/atomic_queue.h` is a snapshot of [`max0x7ba/atomic_queue`](https://github.com/max0x7ba/atomic_queue) (MIT licensed). To update it, copy the header from the upstream repository.

## Generating Documentation

```sh
# Install MkDocs and Material theme
pip install mkdocs-material

# Serve locally with live reload
mkdocs serve

# Build static site
mkdocs build
```

The static site is output to `site/`. Serve at `http://127.0.0.1:8000` during development.
