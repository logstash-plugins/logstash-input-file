# encoding: utf-8
require_relative 'bootstrap' unless defined?(FileWatch)
require "logstash/util/loggable"
module FileWatch
  class Watch
    include LogStash::Util::Loggable

    attr_accessor :lastwarn_max_files
    attr_reader :discoverer, :watched_files_collection

    def initialize(discoverer, watched_files_collection, settings)
      @settings = settings
      # watch and iterate_on_state can be called from different threads.
      @lock = Mutex.new
      # we need to be threadsafe about the quit mutation
      @quit = false
      @quit_lock = Mutex.new
      @lastwarn_max_files = 0
      @discoverer = discoverer
      @watched_files_collection = watched_files_collection
    end

    def add_processor(processor)
      @processor = processor
      @processor.add_watch(self)
      self
    end

    def watch(path)
      synchronized do
        @discoverer.add_path(path)
      end
      # don't return whatever @discoverer.add_path returns
      return true
    end

    def discover
      synchronized do
        @discoverer.discover
      end
      # don't return whatever @discoverer.discover returns
      return true
    end

    def subscribe(dispatcher)
      glob = 0
      interval = @settings.discover_interval
      reset_quit
      until quit?
        iterate_on_state(dispatcher)
        break if quit?
        glob += 1
        if glob == interval
          discover
          glob = 0
        end
        break if quit?
        sleep(@settings.stat_interval)
      end
      @watched_files_collection.close_all
    end # def subscribe

    # Will dispatch to these handlers:
    #   :create_initial - initially present file (so start at end for tail)
    #   :create - file is created (new file after initial globs, start at 0)
    #   :grow   - file has more content
    #   :shrink - file has less content
    #   :delete   - file can't be read
    #   :timeout - file is closable
    #   :unignore - file was ignored, but since then it received new content
    #   see the individual handlers for more info
    def iterate_on_state(handler)
      return if @watched_files_collection.empty?
      synchronized do
        begin
          # creates this snapshot of watched_file values just once
          watched_files = @watched_files_collection.values
          @processor.process_closed(watched_files, handler)
          return if quit?
          @processor.process_ignored(watched_files, handler)
          return if quit?
          @processor.process_watched(watched_files, handler)
          return if quit?
          @processor.process_active(watched_files, handler)
        ensure
          @watched_files_collection.delete(@processor.deletable_filepaths)
          @processor.deletable_filepaths.clear
        end
      end
    end # def each

    def quit
      @quit_lock.synchronize do
        @quit = true
      end
    end # def quit

    def quit?
      @quit_lock.synchronize { @quit }
    end

    private

    def synchronized(&block)
      @lock.synchronize { block.call }
    end

    def reset_quit
      @quit_lock.synchronize { @quit = false }
    end
  end
end
