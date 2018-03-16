# encoding: utf-8
require_relative 'bootstrap'
require_relative 'tail_handlers/processor'
require "logstash/util/loggable"

module FileWatch
  class ObservingTail
    include LogStash::Util::Loggable
    include ObservingBase

    def build_specific_processor(settings)
      TailHandlers::Processor.new(settings)
    end

    def subscribe(observer = NullObserver.new)
      # observer here is the file input
      dispatcher = TailHandlers::Dispatch.new(sincedb_collection, observer, @settings)
      watch.subscribe(dispatcher)
      sincedb_collection.write("subscribe complete - shutting down")
    end
  end
end
