# encoding: utf-8
require_relative 'bootstrap'
require_relative 'tail_handlers/processor'
require "logstash/util/loggable"

module FileWatch
  class ObservingTail
    include LogStash::Util::Loggable
    include ObservingBase

    def build_specific_processor
      TailHandlers::Processor.new
    end

    def subscribe(observer = NullObserver.new)
      # observer here is more than likely the file input
      dispatcher = TailHandlers::Dispatch.new(sincedb_collection, observer)
      watch.subscribe(dispatcher)
      sincedb_collection.write("subscribe complete - shutting down")
    end # def subscribe
  end
end # module FileWatch
