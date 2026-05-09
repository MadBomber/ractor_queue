# SIMD/MIMD Parallel Processing Demos — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `examples/parallel.rb` (shared `Parallel` + `Compute` helpers) and two demo scripts (`05_simd.rb`, `06_pipeline.rb`) that prove RactorQueue's value for CPU-bound parallel workloads by showing measurable speedup over serial Ruby.

**Architecture:** `parallel.rb` defines `Parallel.map` (SIMD: same fn applied to N items via W Ractor workers, results in input order) and `Parallel.pipeline` (MIMD: K stages each with W Ractor workers connected by RactorQueues, stop-pill cascade for shutdown) plus a `Compute` module with all text-processing functions. Demo files `require_relative "parallel"` and compare serial vs. parallel wall time.

**Tech Stack:** Ruby 4.0.2, `ractor_queue` gem (local C extension), `Math` + `etc` stdlib only — no external gems.

**Prerequisite:** Native extension must be compiled before running any file.
```bash
bundle exec rake compile
```

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `examples/parallel.rb` | Create | `Parallel.map`, `Parallel.pipeline`, `Compute` module |
| `examples/05_simd.rb` | Create | SIMD demo: TF-IDF scoring, serial vs. parallel |
| `examples/06_pipeline.rb` | Create | Pipeline demo: chunk → vectorise → rank, serial vs. parallel |

No existing files are modified. No gem library code changes.

---

### Task 1: `Parallel.map` — SIMD helper

**Files:**
- Create: `examples/parallel.rb`

- [ ] **Step 1: Create `examples/parallel.rb` with `Parallel.map`**

```ruby
#!/usr/bin/env ruby
# frozen_string_literal: true

$LOAD_PATH.unshift File.expand_path("../lib", __dir__)
require "ractor_queue"

# ─── Parallel helpers ─────────────────────────────────────────────────────────
#
# Parallel.map(items, workers:, fn:)
#   SIMD: apply fn to every item using W Ractor workers.
#   Returns results in the same order as items.
#   fn must be Ractor.make_shareable-compatible (frozen lambda or module method).
#
# Parallel.pipeline(items, stages:, workers:)
#   MIMD: pass items through K sequential stages, W Ractor workers per stage,
#   connected by RactorQueues. Results order is not guaranteed.
#   Each stage callable must be 1-to-1: one item in, one result out.

module Parallel
  def self.map(items, workers: 4, fn:)
    shareable_fn = Ractor.make_shareable(fn)
    cap     = items.size + workers
    jobs    = RactorQueue.new(capacity: cap)
    results = RactorQueue.new(capacity: cap)

    ractors = workers.times.map do
      Ractor.new(jobs, results, shareable_fn) do |jq, rq, f|
        loop do
          pair = jq.pop
          break if pair == :stop
          idx, item = pair
          rq.push([idx, f.call(item)].freeze)
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
end
```

- [ ] **Step 2: Verify `Parallel.map` returns results in input order**

```bash
bundle exec ruby -e "
require_relative 'examples/parallel'
result = Parallel.map([10, 20, 30, 40, 50], workers: 2, fn: ->(x) { x * 2 })
raise \"expected [20,40,60,80,100], got #{result.inspect}\" unless result == [20, 40, 60, 80, 100]
puts 'Parallel.map: OK'
"
```
Expected output: `Parallel.map: OK`

- [ ] **Step 3: Commit**

```bash
git add examples/parallel.rb
git commit -m "feat(examples): add Parallel.map SIMD helper to parallel.rb"
```

---

### Task 2: `Parallel.pipeline` — MIMD helper

**Files:**
- Modify: `examples/parallel.rb` — add `Parallel.pipeline` inside the `Parallel` module

- [ ] **Step 1: Add `Parallel.pipeline` to the `Parallel` module in `examples/parallel.rb`**

Add the following method inside `module Parallel`, after `Parallel.map`:

```ruby
  def self.pipeline(items, stages:, workers: 4)
    cap         = items.size + workers
    queues      = Array.new(stages.size + 1) { RactorQueue.new(capacity: cap) }
    shareable   = stages.map { |s| Ractor.make_shareable(s) }
    all_ractors = []

    stages.each_with_index do |_, si|
      iq = queues[si]
      oq = queues[si + 1]
      fn = shareable[si]
      workers.times do
        all_ractors << Ractor.new(iq, oq, fn) do |input, output, f|
          loop do
            item = input.pop
            if item == :stop
              output.push(:stop)
              break
            end
            output.push(f.call(item))
          end
        end
      end
    end

    items.each { |item| queues[0].push(item) }
    workers.times { queues[0].push(:stop) }

    results    = queues[-1]
    out        = []
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

    all_ractors.each(&:value)
    out
  end
```

- [ ] **Step 2: Verify `Parallel.pipeline` applies stages in order**

```bash
bundle exec ruby -e "
require_relative 'examples/parallel'
double = ->(x) { x * 2 }
add_one = ->(x) { x + 1 }
result = Parallel.pipeline([1, 2, 3, 4], stages: [double, add_one], workers: 2).sort
raise \"expected [3,5,7,9], got #{result.inspect}\" unless result == [3, 5, 7, 9]
puts 'Parallel.pipeline: OK'
"
```
Expected output: `Parallel.pipeline: OK`

- [ ] **Step 3: Commit**

```bash
git add examples/parallel.rb
git commit -m "feat(examples): add Parallel.pipeline MIMD helper to parallel.rb"
```

---

### Task 3: `Compute` module — tokenize, build_idf, tfidf

**Files:**
- Modify: `examples/parallel.rb` — add `Compute` module after the `Parallel` module

- [ ] **Step 1: Add `Compute` module with shared helpers and TF-IDF functions**

Append the following after the closing `end` of `module Parallel`:

```ruby
# ─── Compute module ───────────────────────────────────────────────────────────
#
# All methods are module methods — naturally Ractor-shareable.
# Pass as frozen lambdas: fn = ->(doc) { Compute.tfidf(doc, idf) }

module Compute
  STOPWORDS = %w[
    a an the and or in of to is it for on with as by at be
    this that these those are was were have has had do does did
    not no but so if then when where what who how
  ].freeze

  # ── Shared ─────────────────────────────────────────────────────────────────

  # Lowercases text, extracts alphabetic tokens, removes stopwords.
  def self.tokenize(text)
    text.downcase.scan(/[a-z]+/).reject { |w| STOPWORDS.include?(w) }
  end

  # ── TF-IDF (used by 05_simd.rb) ────────────────────────────────────────────

  # Builds a frozen IDF table from a corpus Array<String>.
  # Uses add-1 (Laplace) smoothing: idf(t) = log((N+1)/(df(t)+1)) + 1
  def self.build_idf(corpus)
    n  = corpus.size.to_f
    df = Hash.new(0)
    corpus.each { |doc| tokenize(doc).uniq.each { |term| df[term] += 1 } }
    df.to_h { |term, freq| [term, Math.log((n + 1) / (freq + 1)) + 1.0] }.freeze
  end

  # Returns a frozen Hash { term => tf_idf_score } for one document.
  # idf_table must be a frozen Hash from build_idf.
  def self.tfidf(doc, idf_table)
    tokens = tokenize(doc)
    return {}.freeze if tokens.empty?
    tf = tokens.tally.transform_values { |c| c.fdiv(tokens.size) }
    tf.to_h { |term, tf_val| [term, tf_val * idf_table.fetch(term, 1.0)] }.freeze
  end
end
```

- [ ] **Step 2: Verify tokenize and tfidf return expected types**

```bash
bundle exec ruby -e "
require_relative 'examples/parallel'
tokens = Compute.tokenize('The Ruby Ractor runs on a CPU thread')
raise 'stopwords not removed' if tokens.include?('the') || tokens.include?('a') || tokens.include?('on')
raise 'expected ruby in tokens' unless tokens.include?('ruby')

corpus = ['ruby ractor is fast', 'ractor runs on cpu', 'ruby thread mutex lock']
idf = Compute.build_idf(corpus)
raise 'idf not frozen' unless idf.frozen?

scores = Compute.tfidf('ruby ractor', idf)
raise 'scores not frozen' unless scores.frozen?
raise 'ruby missing from scores' unless scores.key?('ruby')
puts 'Compute tokenize/build_idf/tfidf: OK'
"
```
Expected output: `Compute tokenize/build_idf/tfidf: OK`

- [ ] **Step 3: Commit**

```bash
git add examples/parallel.rb
git commit -m "feat(examples): add Compute module with tokenize/build_idf/tfidf"
```

---

### Task 4: `Compute` — pipeline functions + corpus helpers

**Files:**
- Modify: `examples/parallel.rb` — add methods inside `module Compute`

- [ ] **Step 1: Add `chunk`, `vectorise`, `rank`, `build_query_vector`, `load_corpus`, `generate_synthetic_corpus` inside `module Compute`**

Add the following inside `module Compute` (before the final `end`):

```ruby
  VOCAB = %w[
    ruby ractor concurrency thread mutex lock queue producer consumer
    parallel cpu memory async fiber scheduler pipeline stage worker
    channel message actor model deadlock race condition atomic spinlock
    performance throughput latency benchmark optimize garbage collect
    freeze shareable mutable immutable object method block lambda proc
    yield return value result hash array string integer float boolean
    class module include extend require load encode parse transform
  ].freeze

  # ── Pipeline compute functions (used by 06_pipeline.rb) ────────────────────

  # Splits a document into overlapping fixed-size word-window chunks.
  # Returns Array<String> (frozen).
  def self.chunk(doc, size: 40, stride: 20)
    words = doc.split
    return [doc.dup.freeze] if words.size <= size
    chunks = []
    i = 0
    while i < words.size
      chunks << words[i, size].join(" ").freeze
      i += stride
    end
    chunks
  end

  # Computes a TF vector for a chunk.
  # Returns frozen [chunk_text, frozen_tf_hash].
  def self.vectorise(chunk_text)
    tokens = tokenize(chunk_text)
    return [chunk_text.dup.freeze, {}.freeze].freeze if tokens.empty?
    tf = tokens.tally.transform_values { |c| c.fdiv(tokens.size) }
    [chunk_text.dup.freeze, tf.freeze].freeze
  end

  # Scores a vectorised chunk against a query vector via cosine similarity.
  # pair must be [chunk_text, tf_hash] (output of vectorise).
  # query_vector must be a frozen Hash from build_query_vector.
  # Returns frozen [cosine_score, chunk_text].
  def self.rank(pair, query_vector)
    chunk_text, vector = pair
    return [0.0, chunk_text].freeze if vector.empty? || query_vector.empty?
    dot   = query_vector.sum { |term, qw| qw * vector.fetch(term, 0.0) }
    mag_q = Math.sqrt(query_vector.values.sum { |w| w * w })
    mag_v = Math.sqrt(vector.values.sum { |w| w * w })
    score = (mag_q > 0 && mag_v > 0) ? dot / (mag_q * mag_v) : 0.0
    [score, chunk_text].freeze
  end

  # Builds a unit-normalised TF vector from a query string.
  # Returns a frozen Hash { term => weight }.
  def self.build_query_vector(query_string)
    tokens = tokenize(query_string)
    return {}.freeze if tokens.empty?
    tally  = tokens.tally
    total  = tokens.size.to_f
    tally.transform_values { |c| c / total }.freeze
  end

  # ── Corpus helpers ──────────────────────────────────────────────────────────

  # Loads .txt files from dir, or falls back to a synthetic corpus.
  def self.load_corpus(dir = nil)
    if dir
      files = Dir.glob("#{dir}/**/*.txt")
      unless files.empty?
        $stderr.puts "  Loading #{files.size} .txt files from #{dir}"
        return files.map { |f| File.read(f, encoding: "utf-8").freeze }
      end
      $stderr.puts "  No .txt files found in #{dir}; using synthetic corpus."
    end
    generate_synthetic_corpus
  end

  # Generates a reproducible synthetic corpus of doc_count documents,
  # each with words_per_doc words drawn from VOCAB.
  # Each document has a random 5-word "topic" that appears at 40% frequency
  # to create meaningful TF-IDF differentiation across documents.
  def self.generate_synthetic_corpus(doc_count: 200, words_per_doc: 150, seed: 42)
    rng = Random.new(seed)
    doc_count.times.map do
      topic = VOCAB.sample(5, random: rng)
      words = words_per_doc.times.map do
        rng.rand < 0.4 ? topic.sample(random: rng) : VOCAB.sample(random: rng)
      end
      words.join(" ").freeze
    end
  end
```

- [ ] **Step 2: Verify chunk, vectorise, rank, build_query_vector**

```bash
bundle exec ruby -e "
require_relative 'examples/parallel'

# chunk
doc   = 'ruby ractor ' * 30
parts = Compute.chunk(doc.strip)
raise 'chunk returned wrong type' unless parts.is_a?(Array) && parts.all? { |c| c.is_a?(String) }
raise 'chunk parts not frozen' unless parts.all?(&:frozen?)

# vectorise
v = Compute.vectorise('ruby concurrency ractor thread')
raise 'vectorise wrong shape' unless v.size == 2 && v.frozen?
raise 'vector not frozen' unless v[1].frozen?

# build_query_vector
qv = Compute.build_query_vector('ruby ractor')
raise 'query vector not frozen' unless qv.frozen?
raise 'ruby missing from query vector' unless qv.key?('ruby')

# rank
pair = Compute.vectorise('ruby ractor concurrency')
scored = Compute.rank(pair, qv)
raise 'rank wrong shape' unless scored.size == 2 && scored.frozen?
raise 'score not float' unless scored[0].is_a?(Float)
raise 'score out of range' unless scored[0] >= 0.0 && scored[0] <= 1.01

# load_corpus / generate
corpus = Compute.load_corpus
raise 'expected 200 docs' unless corpus.size == 200
raise 'docs not frozen' unless corpus.all?(&:frozen?)
puts 'Compute chunk/vectorise/rank/build_query_vector/load_corpus: OK'
"
```
Expected output: `Compute chunk/vectorise/rank/build_query_vector/load_corpus: OK`

- [ ] **Step 3: Commit**

```bash
git add examples/parallel.rb
git commit -m "feat(examples): add Compute pipeline functions and corpus helpers"
```

---

### Task 5: `examples/05_simd.rb` — SIMD demo

**Files:**
- Create: `examples/05_simd.rb`

- [ ] **Step 1: Create `examples/05_simd.rb`**

```ruby
#!/usr/bin/env ruby
# frozen_string_literal: true

$stdout.sync = true

# examples/05_simd.rb
#
# SIMD Pattern: the same CPU-bound function (TF-IDF scoring) applied to N
# documents in parallel across W Ractor workers via RactorQueue.
#
# Why Ractors beat Threads for this workload:
#   TF-IDF scoring is pure Ruby string/hash work — 100% CPU-bound.
#   Threads share the GVL: only one thread runs Ruby at a time on CPU work.
#   Ractors each have their own GVL: W Ractors run W scoring jobs in parallel.
#   RactorQueue is the only Ruby queue that is always Ractor.shareable?.
#
# Run:
#   bundle exec ruby examples/05_simd.rb              # synthetic 200-doc corpus
#   bundle exec ruby examples/05_simd.rb /path/to/dir # load .txt files from dir

require "etc"
require_relative "parallel"

WORKERS   = Etc.nprocessors
SEPARATOR = "=" * 62

def bench
  t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  yield
  Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0
end

# ─────────────────────────────────────────────────────────────────────────────

puts
puts "TF-IDF Scoring — SIMD Pattern"
puts SEPARATOR
puts "  Workers: #{WORKERS} Ractors  (#{Etc.nprocessors} logical CPU cores)"

corpus = Compute.load_corpus(ARGV[0])
puts "  Corpus:  #{corpus.size} documents"

# Build the IDF table once from the full corpus, then freeze it.
# Workers receive the frozen table via Ractor.make_shareable inside Parallel.map.
idf      = Compute.build_idf(corpus).freeze
tfidf_fn = ->(doc) { Compute.tfidf(doc, idf) }

puts
puts "  Running serial baseline..."
serial_scores = nil
t_serial = bench { serial_scores = corpus.map { |doc| tfidf_fn.call(doc) } }

puts "  Running parallel (#{WORKERS} Ractors)..."
parallel_scores = nil
t_parallel = bench { parallel_scores = Parallel.map(corpus, workers: WORKERS, fn: tfidf_fn) }

# ── Results ───────────────────────────────────────────────────────────────────

puts
puts "  Serial:   #{format('%.3f', t_serial)}s"
puts "  Parallel: #{format('%.3f', t_parallel)}s   (#{format('%.1f', t_serial / t_parallel)}x speedup)"

puts
puts "  Top 5 terms by TF-IDF score in doc[0]:"
top = parallel_scores[0].max_by(5) { |_, v| v }
top.each { |term, score| printf "    %-22s %.4f\n", term, score }

# Verify correctness: parallel results must match serial exactly
mismatches = serial_scores.zip(parallel_scores).count { |s, p| s != p }
puts
if mismatches > 0
  warn "  WARNING: #{mismatches}/#{corpus.size} documents have mismatched results!"
else
  puts "  Correctness: all #{corpus.size} documents match serial output. OK"
end
```

- [ ] **Step 2: Run the demo and verify output**

```bash
bundle exec ruby examples/05_simd.rb
```

Expected output (exact numbers vary by machine):
```
TF-IDF Scoring — SIMD Pattern
==============================================================
  Workers: 12 Ractors  (12 logical CPU cores)
  Corpus:  200 documents

  Running serial baseline...
  Running parallel (12 Ractors)...

  Serial:   0.XXXs
  Parallel: 0.XXXs   (N.Nx speedup)

  Top 5 terms by TF-IDF score in doc[0]:
    <term>                 X.XXXX
    ...

  Correctness: all 200 documents match serial output. OK
```

Verify:
- No errors or exceptions
- Speedup line shows a number > 1.0
- Correctness line says "OK"

- [ ] **Step 3: Commit**

```bash
git add examples/05_simd.rb
git commit -m "feat(examples): add 05_simd.rb SIMD TF-IDF demo"
```

---

### Task 6: `examples/06_pipeline.rb` — pipeline demo

**Files:**
- Create: `examples/06_pipeline.rb`

- [ ] **Step 1: Create `examples/06_pipeline.rb`**

```ruby
#!/usr/bin/env ruby
# frozen_string_literal: true

$stdout.sync = true

# examples/06_pipeline.rb
#
# MIMD Pipeline Pattern: documents pass through 3 processing stages.
#
#   Stage 0 — Chunk:     Parallel.map (SIMD)        — one doc  → many chunks
#   Stage 1 — Vectorise: Parallel.pipeline (MIMD)   — one chunk → [text, vector]
#   Stage 2 — Rank:      Parallel.pipeline (MIMD)   — [text, vector] → [score, text]
#
# Stages 0 and 1 are CPU-bound pure-Ruby work; Ractors bypass the GVL for true
# parallelism. Stage 0 fans out (one doc → N chunks), so it uses Parallel.map
# + flatten rather than Parallel.pipeline (which is strictly 1-to-1).
#
# RactorQueue is the only queue that connects Ractor workers across stages —
# Ruby's built-in Queue uses Mutex internally and is not Ractor.shareable?.
#
# Run:
#   bundle exec ruby examples/06_pipeline.rb              # synthetic corpus
#   bundle exec ruby examples/06_pipeline.rb /path/to/dir # load .txt files

require "etc"
require_relative "parallel"

WORKERS   = Etc.nprocessors
QUERY     = "ruby concurrency ractor".freeze
TOP_N     = 5
SEPARATOR = "=" * 62

def bench
  t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  yield
  Process.clock_gettime(Process::CLOCK_MONOTONIC) - t0
end

# ─────────────────────────────────────────────────────────────────────────────

puts
puts "Semantic Chunk Ranking — Pipeline (MIMD) Pattern"
puts SEPARATOR
puts "  Workers: #{WORKERS} Ractors per stage  (#{Etc.nprocessors} logical CPU cores)"

corpus = Compute.load_corpus(ARGV[0])
puts "  Corpus:  #{corpus.size} documents"
puts "  Query:   #{QUERY.inspect}"

query_vector = Compute.build_query_vector(QUERY).freeze
rank_fn      = ->(pair) { Compute.rank(pair, query_vector) }
chunk_fn     = ->(doc)  { Compute.chunk(doc) }
vectorise_fn = ->(chunk) { Compute.vectorise(chunk) }

# ── Serial baseline ───────────────────────────────────────────────────────────

puts
puts "  Running serial baseline..."
serial_top = nil
t_serial = bench do
  serial_top = corpus
    .flat_map { |doc|   Compute.chunk(doc) }
    .map      { |chunk| Compute.vectorise(chunk) }
    .map      { |pair|  Compute.rank(pair, query_vector) }
    .max_by(TOP_N, &:first)
end

# ── Parallel pipeline ─────────────────────────────────────────────────────────
#
# Step 1: chunk (fan-out) via Parallel.map → flatten
# Step 2: vectorise → rank via Parallel.pipeline

puts "  Running parallel pipeline (#{WORKERS} Ractors per stage)..."
parallel_top = nil
t_parallel = bench do
  chunks       = Parallel.map(corpus, workers: WORKERS, fn: chunk_fn).flatten(1)
  stages       = [vectorise_fn, rank_fn]
  parallel_top = Parallel.pipeline(chunks, stages: stages, workers: WORKERS)
                          .max_by(TOP_N, &:first)
end

# ── Output ────────────────────────────────────────────────────────────────────

puts
puts "  Serial:   #{format('%.3f', t_serial)}s"
puts "  Parallel: #{format('%.3f', t_parallel)}s   (#{format('%.1f', t_serial / t_parallel)}x speedup)"
puts "  Chunks processed: #{corpus.flat_map { |d| Compute.chunk(d) }.size} total"

puts
puts "  Top #{TOP_N} chunks most similar to #{QUERY.inspect}:"
parallel_top.each_with_index do |(score, chunk), i|
  printf "  %d. [%.4f] %s\n", i + 1, score, chunk[0..68]
end

puts
puts "  Note: pipeline result order is not guaranteed (shown after sort by score)."
```

- [ ] **Step 2: Run the demo and verify output**

```bash
bundle exec ruby examples/06_pipeline.rb
```

Expected output (exact numbers vary):
```
Semantic Chunk Ranking — Pipeline (MIMD) Pattern
==============================================================
  Workers: 12 Ractors per stage  (12 logical CPU cores)
  Corpus:  200 documents
  Query:   "ruby concurrency ractor"

  Running serial baseline...
  Running parallel pipeline (12 Ractors per stage)...

  Serial:   X.XXXs
  Parallel: X.XXXs   (N.Nx speedup)
  Chunks processed: NNNN total

  Top 5 chunks most similar to "ruby concurrency ractor":
  1. [0.XXXX] <chunk text preview...>
  ...

  Note: pipeline result order is not guaranteed (shown after sort by score).
```

Verify:
- No exceptions
- Speedup > 1.0
- 5 ranked chunks displayed with cosine scores between 0.0 and 1.0

- [ ] **Step 3: Commit**

```bash
git add examples/06_pipeline.rb
git commit -m "feat(examples): add 06_pipeline.rb MIMD chunk-vectorise-rank demo"
```

---

## Self-review checklist

- [x] **Spec coverage**: `Parallel.map` (Task 1), `Parallel.pipeline` (Task 2), `Compute` TF-IDF (Task 3), `Compute` pipeline functions + corpus helpers (Task 4), `05_simd.rb` (Task 5), `06_pipeline.rb` (Task 6). All spec sections covered.
- [x] **No placeholders**: all steps contain complete, runnable code.
- [x] **Type consistency**: `Compute.vectorise` returns `[chunk_text, tf_hash]`; `Compute.rank` receives `[chunk_text, tf_hash]` and `query_vector` Hash — consistent across Tasks 4, 5, 6. `idf_table` is a frozen Hash from `build_idf`; `tfidf` receives it as second arg — consistent across Tasks 3 and 5.
- [x] **Fan-out handled**: `chunk_fn` returns `Array<String>`; demo uses `Parallel.map(...).flatten(1)` before feeding `Parallel.pipeline` — not a pipeline stage directly. Consistent with spec.
- [x] **Stop-pill count**: `Parallel.pipeline` drain loop waits for exactly `workers` stop pills (W pills cascade through K stages, W arrive at results queue). Correct per spec.
- [x] **Ractor shareability**: `idf` and `query_vector` are frozen before being closed over in lambdas. `Ractor.make_shareable` inside both helpers freezes the lambda and its closed-over data. `validate_shareable: false` (default) allows mutable String corpus items to cross Ractor boundaries under Ruby 4.0 semantics.
