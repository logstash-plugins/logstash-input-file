# encoding: utf-8
require_relative 'bootstrap'
require_relative "read_handlers/processor"
require "logstash/util/loggable"

module FileWatch
  class ObservingRead
    include LogStash::Util::Loggable
    include ObservingBase

    def build_specific_processor(settings)
      ReadHandlers::Processor.new(settings)
    end

    def subscribe(observer = NullObserver.new)
      # observer here is the file input
      dispatcher = ReadHandlers::Dispatch.new(sincedb_collection, observer, @settings)
      watch.subscribe(dispatcher)
      sincedb_collection.write("shutting down")
    end
  end
end
