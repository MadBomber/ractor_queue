class RactorQueue
  include Interface

  # Public sentinel returned by try_pop when the queue is empty.
  # Use equal? (identity) to check — never == — so that nil is an
  # unambiguous payload.
  #
  #   entry = q.try_pop
  #   return if entry.equal?(RactorQueue::EMPTY)
  EMPTY = EMPTY_SENTINEL

  # @param capacity [Integer] Maximum number of elements the queue can hold.
  # @param validate_shareable [Boolean] Raise NotShareableError on non-shareable pushes.
  def self.new(capacity:, validate_shareable: false)
    instance = super(capacity)
    instance.instance_variable_set(:@validate_shareable, validate_shareable)
    # Make the queue instance itself Ractor-shareable. This deep-freezes the Ruby
    # wrapper object. The C++ AtomicQueueB2 buffer is not affected by Ruby's freeze.
    Ractor.make_shareable(instance)
    instance
  end
end
