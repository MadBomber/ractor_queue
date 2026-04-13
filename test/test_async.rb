require "test_helper"
require "ractor_queue"
require "async"

# Tests for async_push and async_pop — the fiber-scheduler-aware blocking
# variants that use sleep(0) on every retry instead of the Thread.pass →
# sleep(100µs) backoff used by push/pop.
#
# All tests run inside Async { } blocks so sleep(0) genuinely yields to the
# reactor. Exceptions raised inside an Async task propagate out of the Async
# call when the reactor finishes, so assert_raises works normally.

class TestAsyncReturnValues < Minitest::Test
  def test_async_push_returns_self
    q = RactorQueue.new(capacity: 4)
    Async { assert_equal q, q.async_push(42) }
  end

  def test_async_pop_returns_value
    q = RactorQueue.new(capacity: 4)
    q.try_push(:hello)
    Async { assert_equal :hello, q.async_pop }
  end

  def test_async_pop_nil_payload_is_unambiguous
    q = RactorQueue.new(capacity: 4)
    q.try_push(nil)
    Async do
      result = q.async_pop
      assert_nil result
      refute result.equal?(RactorQueue::EMPTY), "nil payload must not equal EMPTY"
    end
    # queue is now empty — verify via try_pop rather than a blocking async_pop
    assert q.try_pop.equal?(RactorQueue::EMPTY)
  end
end

class TestAsyncTimeoutZero < Minitest::Test
  TOLERANCE = 0.05

  def test_async_push_timeout_zero_raises_immediately_when_full
    q = RactorQueue.new(capacity: 2)
    fill_queue(q)
    start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    Async { assert_raises(RactorQueue::TimeoutError) { q.async_push(99, timeout: 0) } }
    assert (Process.clock_gettime(Process::CLOCK_MONOTONIC) - start) < TOLERANCE,
      "expected immediate raise"
  end

  def test_async_pop_timeout_zero_raises_immediately_when_empty
    q = RactorQueue.new(capacity: 2)
    start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    Async { assert_raises(RactorQueue::TimeoutError) { q.async_pop(timeout: 0) } }
    assert (Process.clock_gettime(Process::CLOCK_MONOTONIC) - start) < TOLERANCE,
      "expected immediate raise"
  end
end

class TestAsyncTimeoutDeadline < Minitest::Test
  TOLERANCE = 0.05

  def test_async_push_raises_timeout_after_deadline
    q = RactorQueue.new(capacity: 2)
    fill_queue(q)
    start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    Async { assert_raises(RactorQueue::TimeoutError) { q.async_push(99, timeout: 0.1) } }
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start
    assert elapsed >= 0.1,            "expected wait >= 0.1s, got #{elapsed}s"
    assert elapsed < 0.1 + TOLERANCE, "expected raise within tolerance, got #{elapsed}s"
  end

  def test_async_pop_raises_timeout_after_deadline
    q = RactorQueue.new(capacity: 2)
    start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    Async { assert_raises(RactorQueue::TimeoutError) { q.async_pop(timeout: 0.1) } }
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start
    assert elapsed >= 0.1,            "expected wait >= 0.1s, got #{elapsed}s"
    assert elapsed < 0.1 + TOLERANCE, "expected raise within tolerance, got #{elapsed}s"
  end

  def test_async_push_succeeds_within_timeout_when_space_opens
    q = RactorQueue.new(capacity: 2)
    fill_queue(q)
    t = Thread.new { sleep 0.05; q.pop }
    Async { assert_equal q, q.async_push(99, timeout: 0.5) }
    t.join(1)
  end

  def test_async_pop_succeeds_within_timeout_when_element_arrives
    q = RactorQueue.new(capacity: 2)
    t = Thread.new { sleep 0.05; q.push(42) }
    Async { assert_equal 42, q.async_pop(timeout: 0.5) }
    t.join(1)
  end
end

class TestAsyncValidateShareable < Minitest::Test
  def test_async_push_raises_for_non_shareable
    q = RactorQueue.new(capacity: 4, validate_shareable: true)
    Async { assert_raises(RactorQueue::NotShareableError) { q.async_push("mutable".dup) } }
  end

  def test_async_push_accepts_frozen_string
    q = RactorQueue.new(capacity: 4, validate_shareable: true)
    Async { assert q.async_push("frozen".freeze) }
  end

  def test_async_push_accepts_symbol
    q = RactorQueue.new(capacity: 4, validate_shareable: true)
    Async { assert q.async_push(:hello) }
  end

  def test_async_push_accepts_integer
    q = RactorQueue.new(capacity: 4, validate_shareable: true)
    Async { assert q.async_push(42) }
  end
end

class TestAsyncCooperativeFibers < Minitest::Test
  def test_producer_consumer_delivers_all_items
    q        = RactorQueue.new(capacity: 4)
    n        = 10
    received = []

    Async do |task|
      pusher = task.async { n.times { |i| sleep(0.002); q.async_push(i) } }
      popper = task.async { n.times { received << q.async_pop } }
      pusher.wait
      popper.wait
    end

    assert_equal n, received.size
    assert_equal (0...n).to_a, received.sort
  end

  def test_each_push_precedes_its_corresponding_pop
    # Verifies fibers actually interleave: the popper must park via sleep(0)
    # and yield to the pusher before each item is available.
    q   = RactorQueue.new(capacity: 2)
    log = []

    Async do |task|
      pusher = task.async do
        3.times do |i|
          sleep(0.005)
          log << "push #{i}"
          q.async_push(i)
        end
      end

      popper = task.async do
        3.times do
          v = q.async_pop
          log << "pop #{v}"
        end
      end

      pusher.wait
      popper.wait
    end

    assert_equal 6, log.size
    3.times do |i|
      push_pos = log.index("push #{i}")
      pop_pos  = log.index("pop #{i}")
      assert push_pos < pop_pos,
        "push #{i} (log[#{push_pos}]) must precede pop #{i} (log[#{pop_pos}])"
    end
  end

  def test_multiple_async_producers_and_consumers
    n_producers  = 3
    n_consumers  = 3
    per_producer = 20
    total        = n_producers * per_producer
    q            = RactorQueue.new(capacity: 64)
    mu           = Mutex.new
    received     = []

    Async do |task|
      producers = n_producers.times.map do |p|
        task.async do
          (p * per_producer ... (p + 1) * per_producer).each { |i| q.async_push(i) }
        end
      end

      consumers = n_consumers.times.map do
        task.async do
          per_producer.times { mu.synchronize { received << q.async_pop } }
        end
      end

      producers.each(&:wait)
      consumers.each(&:wait)
    end

    assert_equal total, received.size
    assert_equal (0...total).to_a, received.sort
  end
end
