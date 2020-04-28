# encoding: utf-8
require "logstash/util/loggable"
require 'concurrent/atomic/atomic_reference'

module FileWatch
  class Processor
    include LogStash::Util::Loggable

    attr_reader :watch

    def initialize(settings)
      @settings = settings
      @deletable_paths = Concurrent::AtomicReference.new []
    end

    def add_watch(watch)
      @watch = watch
      self
    end

    def clear_deletable_paths
      @deletable_paths.get_and_set []
    end

    def add_deletable_path(path)
      @deletable_paths.get << path
    end

    private

    def error_details(error, watched_file)
      details = { :path => watched_file.path,
                  :exception => error.class,
                  :message => error.message,
                  :backtrace => error.backtrace }
      if logger.debug?
        details[:file] = watched_file
      else
        details[:backtrace] = details[:backtrace].take(8) if details[:backtrace]
      end
      details
    end

  end
end
