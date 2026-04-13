#include <rice/rice.hpp>
#include <atomic_queue/atomic_queue.h>
#include <ruby.h>
#include "standard_queue.h"

using namespace Rice;

// Tell Rice how to mark a StandardQueue during GC.
namespace Rice {
  template<>
  void ruby_mark<StandardQueue>(StandardQueue* data) {
    if (data) data->mark();
  }
}

// Global sentinel — a unique frozen Ruby Object used to signal "queue empty"
// from c_try_pop. Pinned as a permanent GC root so it is never collected.
VALUE g_empty_sentinel = Qnil;  // Set in Init_ractor_queue

extern "C" void Init_ractor_queue() {
  // Marks methods registered via rb_define_method (which Rice uses internally for
  // define_method) as Ractor-safe. Verified working with Rice 4.x on Ruby 4.0 —
  // cross-Ractor queue access passes without Ractor::IsolationError.
  rb_ext_ractor_safe(true);

  // Define RactorQueue as the class itself — wraps AtomicQueueB2<VALUE>.
  // Arg("v").setValue() tells Rice 4.x to treat VALUE as a raw Ruby object
  // pointer (no conversion). Return().setValue() does the same for return values.
  Data_Type<StandardQueue> rb_cRQ = define_class<StandardQueue>("RactorQueue")
    .define_constructor(Constructor<StandardQueue, unsigned>())
    .define_method("c_try_push", &StandardQueue::try_push,
                   Arg("v").setValue())
    .define_method("c_try_pop",  &StandardQueue::try_pop,
                   Return().setValue())
    .define_method("capacity",   &StandardQueue::capacity)
    .define_method("was_size",   &StandardQueue::was_size)
    .define_method("was_empty",  &StandardQueue::was_empty)
    .define_method("was_full",   &StandardQueue::was_full);

  // Create the permanent EMPTY_SENTINEL object and pin it as a GC root.
  g_empty_sentinel = rb_obj_alloc(rb_cObject);
  rb_obj_freeze(g_empty_sentinel);
  rb_gc_register_mark_object(g_empty_sentinel);
  rb_define_const(rb_cRQ, "EMPTY_SENTINEL", g_empty_sentinel);

  // Mark the wrapped C++ type as Ractor-shareable when frozen.
  Data_Type<StandardQueue>::ruby_data_type()->flags |= RUBY_TYPED_FROZEN_SHAREABLE;

  // Restore: methods defined after this point are not auto-marked Ractor-safe.
  rb_ext_ractor_safe(false);
}
