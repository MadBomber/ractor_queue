require "mkmf-rice"

$INCFLAGS << " -I$(srcdir)/../../vendor/atomic_queue/include"
$CPPFLAGS << " -std=c++17"

unless find_header("atomic_queue/atomic_queue.h")
  abort "Cannot find atomic_queue/atomic_queue.h — check vendor/ directory"
end

create_makefile "ractor_queue/ractor_queue"
