require "test_helper"
require "ractor_queue"

class TestRactorQueueInterface < Minitest::Test
  def setup
    @q = RactorQueue.new(capacity: 4)
  end

  def test_try_push_returns_true_when_space_available
    assert_equal true, @q.try_push(42)
  end

  def test_try_push_returns_false_when_full
    fill_queue(@q)
    assert_equal false, @q.try_push(99)
  end

  def test_try_pop_returns_value_when_available
    @q.c_try_push(42)
    assert_equal 42, @q.try_pop
  end

  def test_try_pop_returns_empty_when_queue_is_empty
    result = @q.try_pop
    assert result.equal?(RactorQueue::EMPTY), "expected EMPTY sentinel, got #{result.inspect}"
  end

  def test_try_pop_nil_payload_is_unambiguous
    @q.c_try_push(nil)
    result = @q.try_pop
    # nil payload comes back as nil — not EMPTY — so the two are distinguishable
    assert_nil result
    refute result.equal?(RactorQueue::EMPTY)
    # queue is now empty — next try_pop returns EMPTY
    assert @q.try_pop.equal?(RactorQueue::EMPTY)
  end

  def test_empty_constant_is_ractor_shareable
    assert Ractor.shareable?(RactorQueue::EMPTY)
  end

  def test_empty_constant_is_same_object_as_empty_sentinel
    assert RactorQueue::EMPTY.equal?(RactorQueue::EMPTY_SENTINEL)
  end

  def test_size_reflects_elements
    @q.try_push(1)
    @q.try_push(2)
    assert_equal 2, @q.size
  end

  def test_empty_predicate
    assert @q.empty?
    @q.try_push(1)
    refute @q.empty?
  end

  def test_full_predicate
    refute @q.full?
    fill_queue(@q)
    assert @q.full?
  end

  def test_capacity
    assert @q.capacity >= 4, "expected capacity >= 4, got #{@q.capacity}"
  end
end

class TestErrors < Minitest::Test
  def test_not_shareable_error_is_ractor_queue_error
    assert RactorQueue::NotShareableError < RactorQueue::Error
  end

  def test_timeout_error_is_ractor_queue_error
    assert RactorQueue::TimeoutError < RactorQueue::Error
  end

  def test_error_is_standard_error
    assert RactorQueue::Error < StandardError
  end
end

class TestRactorQueueBlocking < Minitest::Test
  def setup
    @q = RactorQueue.new(capacity: 4)
  end

  def test_push_returns_self
    result = @q.push(42)
    assert_equal @q, result
  end

  def test_pop_returns_element
    @q.push(:hello)
    assert_equal :hello, @q.pop
  end

  def test_push_with_timeout_raises_when_full
    fill_queue(@q)
    assert_raises(RactorQueue::TimeoutError) { @q.push(99, timeout: 0.001) }
  end

  def test_pop_with_timeout_raises_when_empty
    assert_raises(RactorQueue::TimeoutError) { @q.pop(timeout: 0.001) }
  end

  def test_validate_shareable_raises_for_non_shareable
    q = RactorQueue.new(capacity: 4, validate_shareable: true)
    mutable_obj = "mutable string".dup
    assert_raises(RactorQueue::NotShareableError) { q.try_push(mutable_obj) }
  end

  def test_validate_shareable_allows_frozen_string
    q = RactorQueue.new(capacity: 4, validate_shareable: true)
    frozen_obj = "frozen".freeze
    assert q.try_push(frozen_obj)
  end

  def test_validate_shareable_allows_integers
    q = RactorQueue.new(capacity: 4, validate_shareable: true)
    assert q.try_push(42)
  end
end

class TestRactorQueueFactory < Minitest::Test
  def test_new_returns_ractor_queue_instance
    q = RactorQueue.new(capacity: 8)
    assert_instance_of RactorQueue, q
  end

  def test_instance_is_ractor_shareable
    q = RactorQueue.new(capacity: 8)
    assert Ractor.shareable?(q)
  end

  def test_validate_shareable_false_by_default
    q = RactorQueue.new(capacity: 8)
    # Mutable object (not shareable) can be pushed without error
    assert q.try_push("mutable string")
  end

  def test_validate_shareable_raises_for_non_shareable
    q = RactorQueue.new(capacity: 8, validate_shareable: true)
    assert_raises(RactorQueue::NotShareableError) { q.try_push("mutable string") }
  end

  def test_validate_shareable_accepts_frozen_string
    q = RactorQueue.new(capacity: 8, validate_shareable: true)
    assert q.try_push("frozen".freeze)
  end

  def test_validate_shareable_accepts_integer
    q = RactorQueue.new(capacity: 8, validate_shareable: true)
    assert q.try_push(42)
  end

  def test_validate_shareable_accepts_symbol
    q = RactorQueue.new(capacity: 8, validate_shareable: true)
    assert q.try_push(:hello)
  end
end

class TestRactorQueueBlockingThreads < Minitest::Test
  def test_push_blocks_until_space_available
    q = RactorQueue.new(capacity: 2)
    fill_queue(q)  # fills q.capacity slots (4096 in practice)

    popped = nil
    pusher = Thread.new { q.push(1); popped = 1 }
    sleep 0.02  # give pusher time to start spinning
    q.pop        # make space
    joined = pusher.join(2)
    assert joined, "push did not complete after space was made available"
    assert_equal 1, popped
  end

  def test_pop_blocks_until_element_available
    q = RactorQueue.new(capacity: 2)
    result = nil
    consumer = Thread.new { result = q.pop }
    sleep 0.01
    assert_nil result  # still blocked

    q.push(99)
    consumer.join(1)
    assert_equal 99, result
  end

  def test_push_returns_self
    q = RactorQueue.new(capacity: 4)
    assert_equal q, q.push(1)
  end

  def test_thread_raise_interrupts_blocked_pop
    q = RactorQueue.new(capacity: 2)
    error = nil
    t = Thread.new do
      begin
        q.pop
      rescue RuntimeError => e
        error = e
      end
    end
    sleep 0.01
    t.raise(RuntimeError, "interrupted")
    assert t.join(2), "thread did not terminate — Thread#raise may not have escaped the spin loop"
    assert_equal "interrupted", error.message
  end
end
