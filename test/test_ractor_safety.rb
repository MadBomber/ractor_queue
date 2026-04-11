require "test_helper"
require "ractor_queue"

class TestRactorSafety < Minitest::Test
  def test_queue_is_ractor_shareable
    q = RactorQueue.new(capacity: 4)
    assert Ractor.shareable?(q)
  end

  def test_validate_shareable_raises_for_mutable_string
    q = RactorQueue.new(capacity: 4, validate_shareable: true)
    assert_raises(RactorQueue::NotShareableError) { q.try_push("mutable") }
  end

  def test_validate_shareable_raises_for_mutable_array
    q = RactorQueue.new(capacity: 4, validate_shareable: true)
    assert_raises(RactorQueue::NotShareableError) { q.try_push([1, 2, 3]) }
  end

  def test_validate_shareable_accepts_frozen_string
    q = RactorQueue.new(capacity: 4, validate_shareable: true)
    assert_equal true, q.try_push("frozen".freeze)
  end

  def test_validate_shareable_accepts_symbol
    q = RactorQueue.new(capacity: 4, validate_shareable: true)
    assert_equal true, q.try_push(:hello)
  end

  def test_validate_shareable_accepts_integer
    q = RactorQueue.new(capacity: 4, validate_shareable: true)
    assert_equal true, q.try_push(42)
  end

  def test_validate_shareable_accepts_nil
    q = RactorQueue.new(capacity: 4, validate_shareable: true)
    assert_equal true, q.try_push(nil)
  end

  def test_cross_ractor_round_trip_with_integer
    q = RactorQueue.new(capacity: 4, validate_shareable: true)

    producer = Ractor.new(q) do |queue|
      queue.push(42)
    end

    consumer = Ractor.new(q) do |queue|
      queue.pop(timeout: 5)
    end

    producer.value
    result = consumer.value
    assert_equal 42, result
  end

  def test_cross_ractor_round_trip_with_frozen_string
    q = RactorQueue.new(capacity: 4, validate_shareable: true)
    msg = "hello from ractor".freeze

    producer = Ractor.new(q, msg) do |queue, m|
      queue.push(m)
    end

    consumer = Ractor.new(q) do |queue|
      queue.pop(timeout: 5)
    end

    producer.value
    result = consumer.value
    assert_equal "hello from ractor", result
  end

  def test_pop_of_mutable_object_in_ractor_does_not_raise_in_ruby_4
    # Ruby 4.0 changed Ractor semantics: unshareable values no longer raise
    # Ractor::IsolationError when crossing Ractor boundaries via r.value.
    # In Ruby 3.x this would raise Ractor::IsolationError; Ruby 4.0 allows it.
    # validate_shareable: false (default) permits pushing mutable objects.
    q = RactorQueue.new(capacity: 4)  # validate_shareable: false (default)
    q.push("mutable string")

    r = Ractor.new(q) do |queue|
      queue.pop(timeout: 5)
    end

    result = r.value
    assert_equal "mutable string", result
  end
end
