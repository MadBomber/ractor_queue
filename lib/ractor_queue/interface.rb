class RactorQueue
  module Interface
    # Non-blocking push. Returns true if enqueued, false if full.
    def try_push(obj)
      validate_shareable!(obj) if @validate_shareable
      c_try_push(obj)
    end

    # Non-blocking pop. Returns the object, or RactorQueue::EMPTY if the queue
    # is empty. EMPTY is a unique frozen sentinel distinct from nil, so nil is
    # an unambiguous payload value.
    #
    #   entry = q.try_pop
    #   if entry.equal?(RactorQueue::EMPTY)
    #     # queue was empty
    #   else
    #     process(entry)   # entry may be nil if nil was pushed
    #   end
    def try_pop
      c_try_pop
    end

    # Blocking push. Spins until space is available. Returns self.
    # Raises RactorQueue::TimeoutError if timeout expires.
    # Ruby interrupt-aware via Thread.pass between retries.
    def push(obj, timeout: nil)
      validate_shareable!(obj) if @validate_shareable
      blocking_push(obj, timeout)
    end

    # Blocking pop. Spins until an element is available. Returns the object.
    # Raises RactorQueue::TimeoutError if timeout expires.
    def pop(timeout: nil)
      blocking_pop(timeout)
    end

    # Fiber-scheduler-aware pop. Yields to the async reactor on every empty
    # check via sleep(0) rather than spinning with Thread.pass first.
    # Use inside Async { } blocks. Degrades gracefully to a near-no-op sleep
    # in plain Thread context (no scheduler installed).
    # Raises RactorQueue::TimeoutError if timeout expires.
    def async_pop(timeout: nil)
      deadline = timeout ? Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout : nil
      loop do
        result = c_try_pop
        return result unless result.equal?(EMPTY_SENTINEL)
        raise TimeoutError if deadline && Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
        sleep(0)
      end
    end

    # Fiber-scheduler-aware push. Yields to the async reactor on every full
    # check via sleep(0) rather than spinning with Thread.pass first.
    # Use inside Async { } blocks. Degrades gracefully in plain Thread context.
    # Raises RactorQueue::TimeoutError if timeout expires.
    def async_push(obj, timeout: nil)
      validate_shareable!(obj) if @validate_shareable
      deadline = timeout ? Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout : nil
      loop do
        return self if c_try_push(obj)
        raise TimeoutError if deadline && Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
        sleep(0)
      end
    end

    # Approximate current element count.
    def size    = was_size

    # True if queue appears empty (approximate).
    def empty?  = was_empty

    # True if queue appears full (approximate).
    def full?   = was_full

    private

    def validate_shareable!(obj)
      raise NotShareableError, "#{obj.inspect} is not Ractor-shareable" \
        unless Ractor.shareable?(obj)
    end

    # Ruby-level spin loop with exponential backoff.
    #
    # Phase 1 — Thread.pass (spins 0..SPIN_THRESHOLD-1):
    #   Cheap busy-wait. Fast when the queue clears quickly (light contention).
    #   Triggers Ruby interrupt checking, so Thread#raise / Ctrl-C can escape.
    #
    # Phase 2 — sleep(SLEEP_INTERVAL) after SPIN_THRESHOLD passes:
    #   Suspends the OS thread rather than calling sched_yield. Critical inside
    #   Ractors: each Ractor IS its own OS thread, so Thread.pass only calls
    #   sched_yield, which under high contention just rotates threads at the
    #   same priority without making progress. sleep() actually yields the core,
    #   preventing spin-wait storms when many Ractors share a full/empty queue.
    #
    # NOTE: timeout: 0 means "try once, raise if not immediately successful."
    # The operation is attempted before the deadline check on the first iteration,
    # so a single try is always made (never a pure no-op raise).
    SPIN_THRESHOLD = 16
    SLEEP_INTERVAL = 0.0001  # 100 µs — short enough to keep latency low

    def blocking_push(obj, timeout)
      deadline = timeout ? Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout : nil
      spins = 0
      loop do
        return self if c_try_push(obj)
        raise TimeoutError if deadline && Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
        if spins < SPIN_THRESHOLD
          spins += 1
          Thread.pass  # ~100ns cooperative yield; works in Ractors and Threads
        else
          sleep(SLEEP_INTERVAL)
        end
      end
    end

    # Ruby-level spin loop for blocking pop (same backoff strategy).
    # NOTE: timeout: 0 tries once before checking the deadline.
    def blocking_pop(timeout)
      deadline = timeout ? Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout : nil
      spins = 0
      loop do
        result = c_try_pop
        # No sentinel conversion here — only the sentinel (empty queue) continues the loop;
        # any actual value (including nil) is returned as-is, consistent with try_pop behavior.
        return result unless result.equal?(RactorQueue::EMPTY_SENTINEL)
        raise TimeoutError if deadline && Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
        if spins < SPIN_THRESHOLD
          spins += 1
          Thread.pass  # ~100ns cooperative yield; works in Ractors and Threads
        else
          sleep(SLEEP_INTERVAL)
        end
      end
    end
  end
end
