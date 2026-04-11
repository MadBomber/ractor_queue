$LOAD_PATH.unshift File.expand_path("../lib", __dir__)
require "minitest/autorun"
# Each test file requires its own deps directly (avoids load-order issues
# while source files are being built incrementally across tasks).

def fill_queue(q, value: 1)
  q.capacity.times { q.c_try_push(value) }
end

# drain_queue depends on RactorQueue::EMPTY_SENTINEL, which is defined by the
# native extension (loaded via: require "ractor_queue/ractor_queue").
def drain_queue(q)
  count = 0
  count += 1 while q.c_try_pop != RactorQueue::EMPTY_SENTINEL
  count
end
