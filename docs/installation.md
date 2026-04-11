# Installation

## Requirements

| Requirement | Version |
|---|---|
| MRI Ruby | 3.2 or later |
| C++ compiler | C++17 support required (GCC 7+, Clang 5+, MSVC 2017+) |
| Rice gem | ~> 4.0 (declared as a runtime dependency) |

The native extension is compiled automatically when the gem is installed.

## Gemfile

```ruby
gem "ractor_queue"
```

Then run:

```sh
bundle install
```

## Direct Install

```sh
gem install ractor_queue
```

## Verify the Installation

```ruby
require "ractor_queue"

q = RactorQueue.new(capacity: 64)
puts Ractor.shareable?(q)  # => true
puts q.capacity             # => 4096 (minimum, rounded up to power of two)
```

## Compiler Notes

The gem requires a C++17 compiler to build `max0x7ba/atomic_queue` (vendored header-only library). Most modern systems have one available:

| Platform | Compiler |
|---|---|
| macOS | Xcode Command Line Tools (Clang) |
| Ubuntu / Debian | `apt install build-essential` |
| Fedora / RHEL | `dnf groupinstall "Development Tools"` |
| Windows | MSVC via Visual Studio Build Tools |

## Capacity Rounding

Capacity is always rounded up to the next power of two, with a platform-dependent minimum. On Apple Silicon (arm64), the minimum allocated capacity is **4096** regardless of the value you specify:

```ruby
RactorQueue.new(capacity: 8).capacity    # => 4096 (minimum)
RactorQueue.new(capacity: 1024).capacity # => 4096 (minimum)
RactorQueue.new(capacity: 5000).capacity # => 8192 (next power of two)
```

Size queues for your expected peak working set, not exact item counts.
