require "ractor_queue/ractor_queue.#{RbConfig::CONFIG['DLEXT']}"   # native C extension (Standard, EMPTY_SENTINEL)
require_relative "ractor_queue/version"
require_relative "ractor_queue/errors"
require_relative "ractor_queue/interface"
require_relative "ractor_queue/ractor_queue"  # Ruby layer (factory)
