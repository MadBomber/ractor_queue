# SIMD/MIMD Parallel Processing Helpers — Design Spec

**Date:** 2026-04-12
**Scope:** Example demo apps only (`examples/`). Not part of the `ractor_queue` gem itself.

---

## Overview

Add two example demo applications that showcase RactorQueue's value for CPU-bound parallel workloads:

- `examples/05_simd.rb` — SIMD pattern: same TF-IDF scoring function applied to N documents across W Ractor workers
- `examples/06_pipeline.rb` — MIMD/pipeline pattern: chunk → vectorise → rank stages connected by RactorQueues

Both demos share a `examples/parallel.rb` helper module that provides reusable `Parallel.map` (SIMD) and `Parallel.pipeline` (MIMD) abstractions built on top of `RactorQueue`.

**The story these demos tell:**
> CPU-bound stages (text processing, scoring) saturate the GVL — Threads can't parallelize them. Ractors can. I/O-bound stages release the GVL — Threads are fine there. RactorQueue is the only queue that can bridge both worlds in a single pipeline.

---

## File Structure

```
examples/
  parallel.rb          # Parallel module: .map and .pipeline helpers + Compute module
  05_simd.rb           # SIMD demo: TF-IDF scoring across N documents
  06_pipeline.rb       # MIMD demo: chunk → vectorise → rank pipeline
```

No new directories. No changes to the gem itself.

---

## Public API (`parallel.rb`)

```ruby
# SIMD: apply fn to every item in parallel, return results in input order
Parallel.map(items, workers: 4, fn:)   # => Array (same order as items)

# MIMD: pass items through K sequential stages, each with W workers
Parallel.pipeline(items, stages:, workers: 4)   # => Array (order not guaranteed)
```

### Constraints on callables

- `fn` and each element of `stages` must be `Ractor.make_shareable`-compatible: module methods (`method(:name)`) or frozen lambdas.
- Callables must be strictly 1-to-1: one item in, one result out. Fan-out (one document → many chunks) is handled as a pre-processing step using `Parallel.map`, not inside `Parallel.pipeline`.

---

## `Parallel.map` Internals (SIMD)

```
items ──push──► jobs_queue (capacity: items.size + workers)
                    │
          ┌─────────┴──── ... ────┐
        Ractor                 Ractor        (W workers)
          │                       │
          └─────────┬──── ... ────┘
                    ▼
             results_queue (capacity: items.size + workers)
                    │
              drain → reorder → return
```

- Items are pushed as frozen `[index, item]` pairs so results can be reordered to match input order.
- Workers pop pairs, call `fn.call(item)`, push `[index, result]`.
- Queue capacities = `items.size + workers` — large enough to hold all items upfront; no deadlock risk.
- After all items are pushed, W `:stop` pills are pushed (one per worker). Workers exit on `:stop`.
- Main thread drains the results queue after all Ractors finish (`ractors.each(&:value)`), then reorders into a result array indexed by original position.

```ruby
def self.map(items, workers: 4, fn:)
  shareable_fn = Ractor.make_shareable(fn)
  jobs    = RactorQueue.new(capacity: items.size + workers)
  results = RactorQueue.new(capacity: items.size + workers)

  ractors = workers.times.map do
    Ractor.new(jobs, results, shareable_fn) do |jq, rq, f|
      loop do
        pair = jq.pop
        break if pair == :stop
        idx, item = pair
        rq.push([idx, f.call(item)])
      end
    end
  end

  items.each_with_index { |item, i| jobs.push([i, item].freeze) }
  workers.times { jobs.push(:stop) }
  ractors.each(&:value)

  out = Array.new(items.size)
  loop do
    v = results.try_pop
    break if v.equal?(RactorQueue::EMPTY)
    out[v[0]] = v[1]
  end
  out
end
```

---

## `Parallel.pipeline` Internals (MIMD)

```
items ──► q[0] ──► [W Ractors: stage 0] ──► q[1] ──► [W Ractors: stage 1] ──► ... ──► q[K] (results)
```

- K stages, W workers per stage, K+1 queues (including results queue).
- Each queue capacity = `items.size + workers` (two-queue deadlock prevention: no stage can block on push while upstream is still producing).
- Workers read from `q[i]`, apply stage function, push to `q[i+1]`.

### Shutdown (stop pill cascade)

Main pushes W `:stop` pills into `q[0]`. Each stage worker that receives a `:stop` pushes one `:stop` into the next queue before exiting. Pills cascade automatically through all K stages — main does not need to know the stage count.

With W workers per stage and K stages, exactly W `:stop` pills arrive at the results queue. The drain loop collects all real results and discards all W stop pills:

```ruby
stop_count = 0
loop do
  v = results.pop(timeout: 60)
  if v == :stop
    stop_count += 1
    break if stop_count == workers
  else
    out << v
  end
end
```

---

## Compute Functions (`parallel.rb`, `Compute` module)

All functions are module methods — naturally shareable; passed as `method(:fn_name)` references.

### Shared

```ruby
module Compute
  STOPWORDS = %w[a an the and or in of to is it for on with as by].freeze

  def self.tokenize(text)
    text.downcase.scan(/[a-z]+/).reject { |w| STOPWORDS.include?(w) }
  end
end
```

### For `05_simd.rb` (TF-IDF)

```ruby
def self.build_idf(corpus)
  # returns frozen Hash { term => idf_score } computed over the full corpus
end

def self.tfidf(doc, idf_table)
  # Input:  String document, frozen idf_table Hash
  # Output: frozen Hash { term => tf_idf_score }
  tokens = tokenize(doc)
  tf = tokens.tally.transform_values { |c| c.fdiv(tokens.size) }
  tf.to_h { |term, tf_val| [term, tf_val * idf_table.fetch(term, 0.0)] }.freeze
end
```

The IDF table is built once from the corpus before workers start, frozen, then passed into every worker via `Ractor.make_shareable`. The SIMD `fn` is a lambda closing over it:

```ruby
idf = Compute.build_idf(corpus).freeze
fn  = ->(doc) { Compute.tfidf(doc, idf) }
```

### For `06_pipeline.rb` (chunk → vectorise → rank)

```ruby
def self.chunk(doc, size: 40, stride: 20)
  # Input:  String document
  # Output: Array<String> — overlapping word-window chunks (fan-out via Parallel.map + flatten)
end

def self.vectorise(chunk_text)
  # Input:  String chunk
  # Output: frozen [chunk_text, tfidf_vector_hash]
end

def self.rank(pair, query_vector)
  # Input:  [chunk_text, tfidf_vector_hash], frozen query_vector Hash
  # Output: frozen [cosine_score, chunk_text]
end

def self.build_query_vector(query_string)
  # Tokenize query string and build a unit-normalised TF vector for cosine comparison
  # Output: frozen Hash { term => weight }
end
```

**Fan-out strategy:** `Parallel.pipeline` is strictly 1-to-1. Chunking (1 doc → N chunks) is done as a pre-processing step using `Parallel.map`, then flattened before feeding the pipeline:

```ruby
chunks = Parallel.map(corpus, workers: 4, fn: method(:Compute.chunk)).flatten(1)
# chunks is now Array<String>; feed into vectorise → rank pipeline
```

The query vector is built once, frozen, and closed over in a lambda:

```ruby
query   = Compute.build_query_vector("ruby concurrency ractor").freeze
rank_fn = ->(pair) { Compute.rank(pair, query) }
```

---

## Demo File Structure

Both demos follow the same three-act structure:

```
1. Setup      — load corpus (CLI dir or synthetic fallback), build shared data
2. Serial run — process with plain Ruby map/loop, record wall time
3. Parallel   — same result via Parallel.map or Parallel.pipeline, record wall time
4. Output     — speedup ratio, sample top results
```

### `05_simd.rb`

```ruby
corpus  = load_corpus(ARGV[0])           # Array<String>
idf     = Compute.build_idf(corpus).freeze
fn      = ->(doc) { Compute.tfidf(doc, idf) }

t0 = bench { serial_scores   = corpus.map(&fn) }
t1 = bench { parallel_scores = Parallel.map(corpus, workers: 4, fn: fn) }

puts "Serial:   #{t0.round(2)}s"
puts "Parallel: #{t1.round(2)}s  (#{(t0 / t1).round(1)}× speedup)"
puts "\nTop 5 terms in doc[0]: #{parallel_scores[0].max_by(5) { |_, v| v }.map(&:first).join(', ')}"
```

### `06_pipeline.rb`

```ruby
corpus  = load_corpus(ARGV[0])
query   = Compute.build_query_vector("ruby concurrency ractor").freeze
rank_fn = ->(pair) { Compute.rank(pair, query) }

# Serial baseline: chunk → vectorise → rank in plain Ruby
t0 = bench do
  serial_top = corpus
    .flat_map { |doc| Compute.chunk(doc) }
    .map      { |chunk| Compute.vectorise(chunk) }
    .map      { |pair|  Compute.rank(pair, query) }
    .max_by(10, &:first)
end

# Parallel: Parallel.map for fan-out chunking, then Parallel.pipeline for vectorise → rank
t1 = bench do
  chunks       = Parallel.map(corpus, workers: 4, fn: method(:Compute.chunk)).flatten(1)
  stages       = [method(:Compute.vectorise), rank_fn]
  parallel_top = Parallel.pipeline(chunks, stages: stages, workers: 4).max_by(10, &:first)
end

puts "Serial:   #{t0.round(2)}s"
puts "Parallel: #{t1.round(2)}s  (#{(t0 / t1).round(1)}× speedup)"
puts "\nTop result: #{parallel_top.first[1][0..80]}"
```

This demo shows both helpers in combination: `Parallel.map` for the fan-out stage (SIMD) and `Parallel.pipeline` for the sequential processing stages (MIMD).

---

## Synthetic Corpus

When no CLI directory is given, both demos generate ~200 documents of ~150 words each from a fixed vocabulary (no external dependencies). Corpus generation is deterministic (seeded RNG) so results are reproducible. The synthetic corpus is large enough to show measurable speedup on 4 Ractor workers.

```ruby
def load_corpus(dir = nil)
  return Dir.glob("#{dir}/**/*.txt").map { File.read(_1) } if dir && Dir.exist?(dir)
  generate_synthetic_corpus(doc_count: 200, words_per_doc: 150)
end
```

---

## Error Handling

- Missing CLI directory: fall back silently to synthetic corpus (print a note to stderr).
- Ractor errors: propagate naturally via `ractor.value` — any exception in a worker re-raises on the calling thread.
- Pipeline timeout: `results.pop(timeout: 60)` raises `RactorQueue::TimeoutError` if the pipeline stalls; this surfaces as a clear error rather than hanging forever.

---

## Testing

These are example demos, not library code — no new test files. The existing test suite covers `Parallel.map` and `Parallel.pipeline` indirectly via the MPMC and Ractor safety tests. Manual verification: run both demos with `bundle exec ruby examples/05_simd.rb` and confirm speedup > 1×.

---

## Out of Scope

- No gem extraction in this iteration (revisit if patterns prove broadly useful).
- No async/fiber variants of `Parallel.map` or `Parallel.pipeline` (CPU-bound work doesn't benefit).
- No ordering guarantee for `Parallel.pipeline` results (document this explicitly in the demo output).
- No progress reporting / TUI (keep demos simple).
