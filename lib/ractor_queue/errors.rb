class RactorQueue
  class Error             < StandardError; end
  class NotShareableError < Error; end
  class TimeoutError      < Error; end
end
