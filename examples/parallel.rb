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
    # Fixed-capacity queues fit in L1 cache regardless of corpus size.
    # Drain thread drains results concurrently so the small queue never fills.
    cap  = 4096
    jobs = RactorQueue.new(capacity: cap)
    res  = RactorQueue.new(capacity: cap)
    # Deliver fn via a one-shot queue: Ractor.make_shareable raises
    # Ractor::IsolationError on any lambda whose `self` is not shareable
    # (e.g. a top-level lambda). RactorQueue carries it across without freezing.
    fn_q = RactorQueue.new(capacity: workers + 1)  # +1: AtomicQueueB2 rounds up to power-of-two
    workers.times { fn_q.push(fn) }

    out   = Array.new(items.size)
    # Drain thread collects exactly items.size results then exits.
    # Running concurrently keeps the results queue from blocking workers.
    drain = Thread.new do
      items.size.times do
        v = res.pop
        out[v[0]] = v[1]
      end
    end

    ractors = workers.times.map do
      Ractor.new(fn_q, jobs, res) do |fq, jq, rq|
        f = fq.pop
        loop do
          pair = jq.pop
          break if pair.equal?(:stop)
          idx, item = pair
          rq.push([idx, f.call(item)].freeze)
        end
      end
    end

    items.each_with_index { |item, i| jobs.push([i, item].freeze) }
    workers.times { jobs.push(:stop) }
    ractors.each(&:value)
    drain.join
    out
  end

  def self.pipeline(items, stages:, workers: 4)
    # Fixed-capacity queues fit in L1 cache regardless of corpus size.
    cap         = 4096
    queues      = Array.new(stages.size + 1) { RactorQueue.new(capacity: cap) }
    # Deliver each stage fn via a one-shot queue (same reason as Parallel.map:
    # Ractor.make_shareable raises Ractor::IsolationError on non-shareable lambdas).
    fn_queues   = stages.map do |s|
      fq = RactorQueue.new(capacity: workers + 1)  # +1: AtomicQueueB2 rounds up to power-of-two
      workers.times { fq.push(s) }
      fq
    end
    all_ractors = []

    stages.each_with_index do |_, si|
      iq = queues[si]
      oq = queues[si + 1]
      fq = fn_queues[si]
      workers.times do
        all_ractors << Ractor.new(fq, iq, oq) do |fn_q, input, output|
          f = fn_q.pop
          loop do
            item = input.pop
            if item.equal?(:stop)
              output.push(:stop)
              break
            end
            # make_shareable moves the result onto the shared heap so the global GC
            # tracks it across the Ractor boundary (per-Ractor GC won't collect it).
            output.push(Ractor.make_shareable(f.call(item)))
          end
        end
      end
    end

    results = queues[-1]
    out     = []

    # Drain thread runs concurrently so the results queue never fills and blocks
    # stage workers. Stops after seeing exactly W stop pills.
    # (Each stage worker: receives 1 :stop → forwards 1 :stop → exits;
    #  after K stages, exactly W stop pills arrive at the results queue.)
    drain = Thread.new do
      stop_count = 0
      loop do
        v = results.pop(timeout: 60)
        if v.equal?(:stop)
          stop_count += 1
          break if stop_count == workers
        else
          out << v
        end
      end
    end

    # NOTE: :stop is the shutdown sentinel — items must not be the Symbol :stop.
    items.each { |item| queues[0].push(item) }
    workers.times { queues[0].push(:stop) }

    begin
      drain.join
    ensure
      # Always join workers: re-raises any worker exception; no-op on success.
      all_ractors.each(&:value)
    end
    out
  end
end

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
  # Frozen Hash gives O(1) lookup and is always Ractor.shareable? (frozen keys
  # are literal strings; values are `true`, a special const). Set#freeze does
  # NOT deep-freeze the backing @hash, making it not Ractor-shareable.
  STOPWORDS_HASH = STOPWORDS.each_with_object({}) { |w, h| h[w] = true }.freeze

  # ── Shared ─────────────────────────────────────────────────────────────────

  # Lowercases text, extracts alphabetic tokens, removes stopwords.
  def self.tokenize(text)
    text.downcase.scan(/[a-z]+/).reject { |w| STOPWORDS_HASH.key?(w) }
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
    tf.to_h { |term, tf_val| [term, tf_val * idf_table.fetch(term, 0.0)] }.freeze
  end

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
  # Returns Array<String> (each chunk frozen).
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
  # Keys are frozen so the pair is fully Ractor.shareable? for Ractor→Ractor transfer.
  def self.vectorise(chunk_text)
    tokens = tokenize(chunk_text)
    return [chunk_text.dup.freeze, {}.freeze].freeze if tokens.empty?
    # Freeze float values so the pair is fully Ractor.shareable? without
    # requiring Ractor.make_shareable to deep-traverse every float in the hash.
    tf = tokens.tally.transform_values { |c| c.fdiv(tokens.size).freeze }
    [chunk_text.dup.freeze, tf.transform_keys!(&:freeze).freeze].freeze
  end

  # Scores a vectorised chunk against a query vector via cosine similarity.
  # pair must be [chunk_text, tf_hash] (output of vectorise).
  # query_vector must be a frozen Hash from build_query_vector.
  # Returns frozen [cosine_score, chunk_text].
  def self.rank(pair, query_vector)
    chunk_text, vector = pair
    return [0.0.freeze, chunk_text].freeze if vector.empty? || query_vector.empty?
    dot   = query_vector.sum { |term, qw| qw * vector.fetch(term, 0.0) }
    mag_q = Math.sqrt(query_vector.values.sum { |w| w * w })
    mag_v = Math.sqrt(vector.values.sum { |w| w * w })
    score = (mag_q > 0 && mag_v > 0) ? dot / (mag_q * mag_v) : 0.0
    [score.freeze, chunk_text].freeze
  end

  # Builds a TF-weighted query vector (weights sum to 1) from a query string.
  # Returns a frozen Hash { term => weight }.
  def self.build_query_vector(query_string)
    tokens = tokenize(query_string)
    return {}.freeze if tokens.empty?
    tally  = tokens.tally
    total  = tokens.size.to_f
    tally.transform_values { |c| (c / total).freeze }.freeze
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

  # Generates a reproducible synthetic corpus.
  # Each document has a random 5-word "topic" appearing at 40% frequency,
  # creating meaningful TF-IDF differentiation across documents.
  def self.generate_synthetic_corpus(doc_count: 20_000, words_per_doc: 500, seed: 42)
    rng = Random.new(seed)
    doc_count.times.map do
      topic = VOCAB.sample(5, random: rng)
      words = words_per_doc.times.map do
        rng.rand < 0.4 ? topic.sample(random: rng) : VOCAB.sample(random: rng)
      end
      words.join(" ").freeze
    end
  end
end
