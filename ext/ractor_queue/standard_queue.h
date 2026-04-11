#pragma once
#include <atomic_queue/atomic_queue.h>
#include <ruby.h>

// Global sentinel — initialized in Init_ractor_queue, returned by try_pop when empty.
// Never a valid user-pushed VALUE; only used by the Ruby layer to detect "empty."
extern VALUE g_empty_sentinel;

class StandardQueue {
  atomic_queue::AtomicQueueB2<VALUE> q_;

public:
  explicit StandardQueue(unsigned capacity) : q_(capacity) {}

  // Non-blocking push. Returns true if element was enqueued, false if full.
  bool try_push(VALUE v) { return q_.try_push(v); }

  // Non-blocking pop. Returns the VALUE if one was available,
  // or g_empty_sentinel if the queue was empty.
  VALUE try_pop() {
    VALUE v;
    return q_.try_pop(v) ? v : g_empty_sentinel;
  }

  unsigned capacity()  const { return q_.capacity(); }
  unsigned was_size()  const { return q_.was_size(); }
  bool     was_empty() const { return q_.was_empty(); }
  bool     was_full()  const { return q_.was_full(); }
};
