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
    # RGENGC write-barrier fix: mark the queue as WB-unprotected so minor GC
    # always scans gc_slots_ and keeps queued young objects alive.
    # Without this, pushing a young VALUE into an OLD StandardQueue creates an
    # untracked old→young reference — minor GC never marks it, the VALUE is
    # collected, and subsequent pops return garbage (crash at scale).
    # We pass `instance` explicitly because rb_gc_writebarrier_unprotect needs
    # the raw Ruby VALUE; there is no way to capture `self` as a VALUE inside
    # a Rice member-function binding without an explicit argument.
    instance._gc_unprotect(instance)
    instance.instance_variable_set(:@validate_shareable, validate_shareable)
    # Make the queue instance itself Ractor-shareable. This deep-freezes the Ruby
    # wrapper object. The C++ AtomicQueueB2 buffer is not affected by Ruby's freeze.
    Ractor.make_shareable(instance)
    instance
  end
end
