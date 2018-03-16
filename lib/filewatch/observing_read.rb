# encoding: utf-8
require_relative 'bootstrap'
require_relative "read_handlers/processor"
require "logstash/util/loggable"

module FileWatch
  class ObservingRead
    include LogStash::Util::Loggable
    include ObservingBase

    def build_specific_processor
      ReadHandlers::Processor.new
    end

    def subscribe(observer = NullObserver.new)
      # observer here is the file input
      dispatcher = ReadHandlers::Dispatch.new(sincedb_collection, observer)
      watch.subscribe(dispatcher)
      sincedb_collection.write("shutting down")
    end
  end
end
