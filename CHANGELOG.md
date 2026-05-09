## [Unreleased]

## [0.2.0] - 2026-05-09

### Added

- `async_push(obj, timeout: nil)` — fiber-scheduler-aware blocking push; yields via `sleep(0)` on every retry so the fiber reactor stays responsive inside `Async { }` blocks or Falcon
- `async_pop(timeout: nil)` — fiber-scheduler-aware blocking pop; same `sleep(0)` cooperative yield strategy
- `test/test_async.rb` — full test coverage for `async_push`/`async_pop`: return values, timeout (zero and deadline), cooperative fiber interleaving, MPMC with Async tasks, `validate_shareable` integration
- `test/test_mpmc.rb` — MPMC correctness tests under plain Threads and Ractors: no-loss, no-duplication, asymmetric producer/consumer counts, small-queue blocking paths
- `examples/parallel.rb` — shared `Parallel.map` (SIMD fan-out) and `Parallel.pipeline` (MIMD multi-stage) helpers built on RactorQueue, plus a `Compute` module with TF-IDF and cosine-similarity text-processing functions
- `examples/05_simd.rb` — SIMD demo: parallel TF-IDF scoring across 12 Ractor workers on a 20k-document corpus; shows 2–3× speedup over serial Ruby on CPU-bound work
- `examples/06_pipeline.rb` — MIMD pipeline demo: 2-stage chunk-and-rank pipeline with 6 Ractors per stage; shows 3× speedup over serial on a semantic-similarity workload

### Changed

- `async` added as a development dependency in gemspec (required by `test_async.rb`)
- Gemfile trimmed to only the dependencies the current codebase actually uses (`async`, `debug_me`, `minitest`)

## [0.1.0] - 2026-04-10

- Initial release
