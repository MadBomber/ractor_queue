require_relative "lib/ractor_queue/version"

Gem::Specification.new do |spec|
  spec.name          = "ractor_queue"
  spec.version       = RactorQueue::VERSION
  spec.authors       = ["Dewayne VanHoozer"]
  spec.email         = ["dvanhoozer@gmail.com"]
  spec.summary       = "Ractor-shareable bounded queue for Ruby parallel workloads"
  spec.description   = "A lock-free MPMC queue that can be shared across Ruby Ractors — " \
                       "the only Ractor-safe bounded queue option since Ruby's built-in " \
                       "Queue uses Mutex and cannot cross Ractor boundaries."
  spec.homepage      = "https://github.com/MadBomber/ractor_queue"
  spec.license       = "MIT"
  spec.required_ruby_version = ">= 3.2.0"

  spec.files = Dir[
    "lib/**/*.rb",
    "ext/**/*.{rb,cpp,h}",
    "vendor/atomic_queue/include/**/*.h",
    "README.md",
    "LICENSE"
  ]

  spec.extensions = ["ext/ractor_queue/extconf.rb"]

  spec.add_dependency "rice", "~> 4.0"

  spec.add_development_dependency "rake-compiler", "~> 1.2"
  spec.add_development_dependency "rake", "~> 13.0"
  spec.add_development_dependency "minitest", "~> 5.0"
end
