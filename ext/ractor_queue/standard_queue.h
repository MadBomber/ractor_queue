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

  // Rotating scan hints for try_push and try_pop.
  //
  // gc_slots are claimed/freed in roughly FIFO order (mirroring the queue),
  // so a hint that advances with each successful operation gives O(1) amortised
  // scan instead of a worst-case O(gc_cap_) scan from 0.
  //
  // push_hint_: position of the last successfully claimed slot + 1.
  //   After a full queue cycle (claim 0..gc_cap_-1, wrap), slot 0 is the first
  //   freed, and the hint is back at 0 — so the next push claim is O(1).
  //
  // pop_hint_: position of the last successfully freed slot + 1.
  //   Without this, all concurrent pop Ractors scan from slot 0, creating a
  //   thundering-herd on the slot-0 cache line that dwarfs any scan savings.
  //   With the hint, each successful pop advances the starting position so
  //   concurrent workers naturally spread across different cache lines.
  std::atomic<unsigned> push_hint_{0};
  std::atomic<unsigned> pop_hint_{0};

public:
  explicit StandardQueue(unsigned capacity)
      : q_(capacity), gc_cap_(q_.capacity()) {
    gc_slots_ = new std::atomic<VALUE>[gc_cap_];
    for (unsigned i = 0; i < gc_cap_; i++)
      gc_slots_[i].store(Qnil, std::memory_order_relaxed);
  }

  ~StandardQueue() { delete[] gc_slots_; }

  // Non-blocking push. Returns true if element was enqueued, false if full.
  //
  // ORDERING: gc_slot is claimed BEFORE pushing to q_. This guarantees that any
  // VALUE in q_ is always covered by a gc_slot — there is no window where an item
  // is in the queue but unprotected from GC. Without this ordering, a concurrent
  // pop could drain the item between q_.try_push and the gc_slot CAS, leaving a
  // stale slot claimed forever. After ~gc_cap_ such races all slots fill up, new
  // pushes lose GC coverage, and minor GC collects in-flight items (crash).
  //
  // FAST-PATH: was_full() exits before touching gc_slots_ on the hot retry path.
  // The rotating push_hint_ makes the CAS scan O(1) amortised: since gc_slots
  // are claimed in the same FIFO order as the queue, the hint always points at
  // (or one step past) the oldest freed slot, which is almost always free.
  bool try_push(VALUE v) {
    if (RB_SPECIAL_CONST_P(v)) {
      // Special consts (fixnum, symbol, true/false/nil) live outside the heap;
      // they are never collected and need no gc_slot.
      return q_.try_push(v);
    }

    // Skip gc_slot work entirely when the queue reports full.
    if (q_.was_full()) return false;

    // Scan from the hint position. O(1) amortised for FIFO workloads.
    unsigned start  = push_hint_.load(std::memory_order_relaxed) % gc_cap_;
    int      claimed = -1;
    for (unsigned i = 0; i < gc_cap_; i++) {
      unsigned idx = (start + i) % gc_cap_;
      VALUE expected = Qnil;
      if (gc_slots_[idx].compare_exchange_strong(
              expected, v,
              std::memory_order_release,
              std::memory_order_relaxed)) {
        claimed = (int)idx;
        push_hint_.store((idx + 1) % gc_cap_, std::memory_order_relaxed);
        break;
      }
    }

    if (claimed < 0) {
      // Every gc_slot is occupied: queue is at capacity.
      return false;
    }

    bool ok = q_.try_push(v);
    if (!ok) {
      // Push failed (race: queue became full between was_full() and here).
      // Release the slot and roll the hint back so the next push re-checks it.
      gc_slots_[claimed].store(Qnil, std::memory_order_release);
      push_hint_.store((unsigned)claimed, std::memory_order_relaxed);
    }
    return ok;
  }

  // Non-blocking pop. Returns the VALUE if one was available,
  // or g_empty_sentinel if the queue was empty.
  //
  // Uses CAS (not load+store) to clear the gc_slot so that two concurrent pops
  // of the same VALUE (pushed twice) always clear two DISTINCT slots. A plain
  // load+store would let both pops target the same slot, leaving the other slot
  // permanently occupied — a slow gc_slot leak that eventually fills all slots.
  //
  // Scans from pop_hint_: under high concurrency (e.g. 12 Ractor workers all
  // calling try_pop), scanning from 0 concentrates every CAS retry on the same
  // cache line — a thundering herd that collapses throughput. The rotating hint
  // naturally spreads concurrent scanners across different cache lines, giving
  // O(1) amortised scan with no coherency storm.
  VALUE try_pop() {
    VALUE v;
    if (!q_.try_pop(v)) return g_empty_sentinel;
    if (!RB_SPECIAL_CONST_P(v)) {
      unsigned start = pop_hint_.load(std::memory_order_relaxed) % gc_cap_;
      for (unsigned i = 0; i < gc_cap_; i++) {
        unsigned idx = (start + i) % gc_cap_;
        VALUE expected = v;
        if (gc_slots_[idx].compare_exchange_strong(
                expected, Qnil,
                std::memory_order_release,
                std::memory_order_relaxed)) {
          pop_hint_.store((idx + 1) % gc_cap_, std::memory_order_relaxed);
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

  // Called once after construction from RactorQueue.new.
  // Marks this object as write-barrier-unprotected so Ruby's generational GC
  // (RGENGC) always scans gc_slots_ during minor GC.
  //
  // Without this, StandardQueue (an OLD object after a few GC cycles) never
  // appears in the minor-GC remembered set. Any YOUNG Ruby objects pushed to
  // the queue would not be traced by minor GC and could be collected before
  // they are popped — causing crashes at any pop site.
  //
  // self_val must be the Ruby VALUE of this wrapper object, passed from the
  // Ruby layer via instance._gc_unprotect(instance).
  void gc_unprotect(VALUE self_val) {
    rb_gc_writebarrier_unprotect(self_val);
  }

  unsigned capacity()  const { return q_.capacity(); }
  unsigned was_size()  const { return q_.was_size(); }
  bool     was_empty() const { return q_.was_empty(); }
  bool     was_full()  const { return q_.was_full(); }
};
