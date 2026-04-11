require "test_helper"
require "ractor_queue"

class TestTimeoutStandard < Minitest::Test
  TOLERANCE = 0.05  # 50ms timing tolerance

  def test_push_timeout_zero_raises_immediately_when_full
    q = RactorQueue.new(capacity: 2)
    fill_queue(q)
    start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    assert_raises(RactorQueue::TimeoutError) { q.push(99, timeout: 0) }
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start
    assert elapsed < TOLERANCE, "Expected immediate raise, took #{elapsed}s"
  end

  def test_pop_timeout_zero_raises_immediately_when_empty
    q = RactorQueue.new(capacity: 2)
    start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    assert_raises(RactorQueue::TimeoutError) { q.pop(timeout: 0) }
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start
    assert elapsed < TOLERANCE, "Expected immediate raise, took #{elapsed}s"
  end

  def test_push_raises_timeout_error_after_n_seconds
    q = RactorQueue.new(capacity: 2)
    fill_queue(q)
    start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    assert_raises(RactorQueue::TimeoutError) { q.push(99, timeout: 0.1) }
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start
    assert elapsed >= 0.1, "Expected to wait at least 0.1s, waited #{elapsed}s"
    assert elapsed < 0.1 + TOLERANCE, "Expected to raise within tolerance, waited #{elapsed}s"
  end

  def test_pop_raises_timeout_error_after_n_seconds
    q = RactorQueue.new(capacity: 2)
    start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    assert_raises(RactorQueue::TimeoutError) { q.pop(timeout: 0.1) }
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start
    assert elapsed >= 0.1, "Expected to wait at least 0.1s, waited #{elapsed}s"
    assert elapsed < 0.1 + TOLERANCE, "Expected to raise within tolerance, waited #{elapsed}s"
  end

  def test_push_succeeds_within_timeout_when_space_opens
    q = RactorQueue.new(capacity: 2)
    fill_queue(q)
    t = Thread.new { sleep 0.05; q.pop }
    assert_equal q, q.push(99, timeout: 0.5)
    t.join(1)
  end

  def test_pop_succeeds_within_timeout_when_element_arrives
    q = RactorQueue.new(capacity: 2)
    t = Thread.new { sleep 0.05; q.push(42) }
    assert_equal 42, q.pop(timeout: 0.5)
    t.join(1)
  end
end
