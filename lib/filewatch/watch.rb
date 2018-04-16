# encoding: utf-8
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

    def subscribe(observer, sincedb_collection)
      @processor.initialize_handlers(sincedb_collection, observer)

      glob = 0
      interval = @settings.discover_interval
      reset_quit
      until quit?
        iterate_on_state
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

    # Read mode processor will handle watched_files in the closed, ignored, watched and active state
    # differently from Tail mode - see the ReadMode::Processor and TailMode::Processor
    def iterate_on_state
      return if @watched_files_collection.empty?
      synchronized do
        begin
          # creates this snapshot of watched_file values just once
          watched_files = @watched_files_collection.values
          @processor.process_closed(watched_files)
          return if quit?
          @processor.process_ignored(watched_files)
          return if quit?
          @processor.process_watched(watched_files)
          return if quit?
          @processor.process_active(watched_files)
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
