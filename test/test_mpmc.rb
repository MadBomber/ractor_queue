require "test_helper"
require "ractor_queue"

# MPMC (Multi-Producer Multi-Consumer) correctness tests.
#
# These tests verify that concurrent pushes and pops produce no item loss
# and no duplication — the fundamental correctness guarantee of the queue.
# Each test uses a disjoint integer range per producer so exact correctness
# (not just count) can be asserted.
#
# Two suites:
#   TestMPMCThreads  — plain Ruby Threads with a Mutex-protected result array
#   TestMPMCRactors  — Ractors with a second RactorQueue as the result channel

class TestMPMCThreads < Minitest::Test
  def test_no_loss_no_duplication
    n_producers  = 4
    n_consumers  = 4
    per_producer = 500
    total        = n_producers * per_producer

    q        = RactorQueue.new(capacity: 256)
    mu       = Mutex.new
    received = []

    consumers = n_consumers.times.map do
      Thread.new do
        loop do
          v = q.pop(timeout: 10)
          break if v == :stop
          mu.synchronize { received << v }
        end
      end
    end

    producers = n_producers.times.map do |p|
      Thread.new do
        (p * per_producer ... (p + 1) * per_producer).each { |i| q.push(i) }
      end
    end

    producers.each(&:join)
    n_consumers.times { q.push(:stop) }
    consumers.each { |t| t.join(15) }

    assert_equal total, received.size, "item count mismatch: expected #{total}, got #{received.size}"
    assert_equal (0...total).to_a, received.sort, "items lost or duplicated"
  end

  def test_small_queue_forces_blocking
    # Capacity 16 guarantees producers block repeatedly, exercising the
    # full spin-wait backoff path under real Thread contention.
    n_producers  = 4
    n_consumers  = 4
    per_producer = 200
    total        = n_producers * per_producer

    q        = RactorQueue.new(capacity: 16)
    mu       = Mutex.new
    received = []

    consumers = n_consumers.times.map do
      Thread.new do
        loop do
          v = q.pop(timeout: 10)
          break if v == :stop
          mu.synchronize { received << v }
        end
      end
    end

    producers = n_producers.times.map do |p|
      Thread.new do
        (p * per_producer ... (p + 1) * per_producer).each { |i| q.push(i) }
      end
    end

    producers.each(&:join)
    n_consumers.times { q.push(:stop) }
    consumers.each { |t| t.join(15) }

    assert_equal total, received.size
    assert_equal (0...total).to_a, received.sort, "items lost or duplicated under blocking"
  end

  def test_asymmetric_producer_consumer_counts
    # More producers than consumers — common real-world topology.
    n_producers  = 8
    n_consumers  = 2
    per_producer = 125
    total        = n_producers * per_producer

    q        = RactorQueue.new(capacity: 128)
    mu       = Mutex.new
    received = []

    consumers = n_consumers.times.map do
      Thread.new do
        loop do
          v = q.pop(timeout: 10)
          break if v == :stop
          mu.synchronize { received << v }
        end
      end
    end

    producers = n_producers.times.map do |p|
      Thread.new do
        (p * per_producer ... (p + 1) * per_producer).each { |i| q.push(i) }
      end
    end

    producers.each(&:join)
    n_consumers.times { q.push(:stop) }
    consumers.each { |t| t.join(15) }

    assert_equal total, received.size
    assert_equal (0...total).to_a, received.sort
  end
end

class TestMPMCRactors < Minitest::Test
  def test_no_loss_no_duplication
    n_producers  = 3
    n_consumers  = 3
    per_producer = 200
    total        = n_producers * per_producer

    q       = RactorQueue.new(capacity: 256)
    results = RactorQueue.new(capacity: total + n_consumers)

    consumers = n_consumers.times.map do
      Ractor.new(q, results) do |jq, rq|
        loop do
          v = jq.pop(timeout: 30)
          break if v == :stop
          rq.push(v)
        end
      end
    end

    producers = n_producers.times.map do |p|
      Ractor.new(q, p, per_producer) do |jq, pid, count|
        (pid * count ... (pid + 1) * count).each { |i| jq.push(i) }
      end
    end

    producers.each(&:value)
    n_consumers.times { q.push(:stop) }
    consumers.each(&:value)

    received = drain(results)

    assert_equal total, received.size, "item count mismatch: expected #{total}, got #{received.size}"
    assert_equal (0...total).to_a, received.sort, "items lost or duplicated"
  end

  def test_small_queue_forces_ractor_blocking
    # Capacity 16 forces Ractors into the backoff spin loop, verifying that
    # OS-thread sleep(0) / sleep(100µs) correctly releases cores under Ractor
    # contention without deadlocking or losing items.
    n_producers  = 4
    n_consumers  = 4
    per_producer = 100
    total        = n_producers * per_producer

    q       = RactorQueue.new(capacity: 16)
    results = RactorQueue.new(capacity: total + n_consumers)

    consumers = n_consumers.times.map do
      Ractor.new(q, results) do |jq, rq|
        loop do
          v = jq.pop(timeout: 30)
          break if v == :stop
          rq.push(v)
        end
      end
    end

    producers = n_producers.times.map do |p|
      Ractor.new(q, p, per_producer) do |jq, pid, count|
        (pid * count ... (pid + 1) * count).each { |i| jq.push(i) }
      end
    end

    producers.each(&:value)
    n_consumers.times { q.push(:stop) }
    consumers.each(&:value)

    received = drain(results)

    assert_equal total, received.size
    assert_equal received.sort, received.uniq.sort, "duplicates detected"
    assert_equal (0...total).to_a, received.sort, "items lost under Ractor contention"
  end

  def test_asymmetric_producer_consumer_counts
    n_producers  = 6
    n_consumers  = 2
    per_producer = 100
    total        = n_producers * per_producer

    q       = RactorQueue.new(capacity: 256)
    results = RactorQueue.new(capacity: total + n_consumers)

    consumers = n_consumers.times.map do
      Ractor.new(q, results) do |jq, rq|
        loop do
          v = jq.pop(timeout: 30)
          break if v == :stop
          rq.push(v)
        end
      end
    end

    producers = n_producers.times.map do |p|
      Ractor.new(q, p, per_producer) do |jq, pid, count|
        (pid * count ... (pid + 1) * count).each { |i| jq.push(i) }
      end
    end

    producers.each(&:value)
    n_consumers.times { q.push(:stop) }
    consumers.each(&:value)

    received = drain(results)

    assert_equal total, received.size
    assert_equal (0...total).to_a, received.sort
  end

  private

  def drain(q)
    items = []
    loop do
      v = q.try_pop
      break if v.equal?(RactorQueue::EMPTY)
      items << v
    end
    items
  end
end
