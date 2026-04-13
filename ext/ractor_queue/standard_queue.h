#pragma once
#include <atomic_queue/atomic_queue.h>
#include <ruby.h>
#include <atomic>

// Global sentinel — initialized in Init_ractor_queue, returned by try_pop when empty.
// Never a valid user-pushed VALUE; only used by the Ruby layer to detect "empty."
extern VALUE g_empty_sentinel;

class StandardQueue {
  atomic_queue::AtomicQueueB2<VALUE> q_;

  // GC shadow: one std::atomic<VALUE> slot per queue capacity entry.
  // When a heap-allocated VALUE is enqueued, we CAS it into a free slot.
  // When it is dequeued, we CAS that slot back to Qnil.
  // The dmark callback marks every non-Qnil slot, keeping queued objects alive.
  //
  // Design constraints:
  //  - No std::mutex: the GC can call dmark during stop-the-world while any
  //    Ruby thread may be paused mid-push/pop, so a mutex could deadlock.
  //  - CAS operations are lock-free: safe from Ractors (no GVL required) and
  //    from Threads; two pushers or a push+pop never corrupt the same slot.
  //  - Array is sized to the actual rounded-up capacity, so there are always
  //    enough slots for the maximum in-flight item count.
  std::atomic<VALUE>* gc_slots_;
  unsigned            gc_cap_;

public:
  explicit StandardQueue(unsigned capacity)
      : q_(capacity), gc_cap_(q_.capacity()) {
    gc_slots_ = new std::atomic<VALUE>[gc_cap_];
    for (unsigned i = 0; i < gc_cap_; i++)
      gc_slots_[i].store(Qnil, std::memory_order_relaxed);
  }

  ~StandardQueue() { delete[] gc_slots_; }

  // Non-blocking push. Returns true if element was enqueued, false if full.
  bool try_push(VALUE v) {
    bool ok = q_.try_push(v);
    if (ok && !RB_SPECIAL_CONST_P(v)) {
      // Claim a free slot. CAS ensures concurrent pushers never double-book.
      for (unsigned i = 0; i < gc_cap_; i++) {
        VALUE expected = Qnil;
        if (gc_slots_[i].compare_exchange_strong(
                expected, v,
                std::memory_order_release,
                std::memory_order_relaxed))
          break;
      }
    }
    return ok;
  }

  // Non-blocking pop. Returns the VALUE if one was available,
  // or g_empty_sentinel if the queue was empty.
  VALUE try_pop() {
    VALUE v;
    if (!q_.try_pop(v)) return g_empty_sentinel;
    if (!RB_SPECIAL_CONST_P(v)) {
      // Release the slot. The VALUE is now on the caller's Ruby stack.
      for (unsigned i = 0; i < gc_cap_; i++) {
        VALUE cur = gc_slots_[i].load(std::memory_order_acquire);
        if (cur == v) {
          gc_slots_[i].store(Qnil, std::memory_order_release);
          break;
        }
      }
    }
    return v;
  }

  // Called by the GC dmark callback. Marks every occupied gc_slot so that
  // queued heap-allocated objects survive the current GC cycle.
  // GC is stop-the-world: no concurrent push/pop is possible here, so the
  // relaxed loads are safe.
  void mark() const noexcept {
    for (unsigned i = 0; i < gc_cap_; i++) {
      VALUE v = gc_slots_[i].load(std::memory_order_relaxed);
      if (!RB_SPECIAL_CONST_P(v)) rb_gc_mark(v);
    }
  }

  unsigned capacity()  const { return q_.capacity(); }
  unsigned was_size()  const { return q_.was_size(); }
  bool     was_empty() const { return q_.was_empty(); }
  bool     was_full()  const { return q_.was_full(); }
};
