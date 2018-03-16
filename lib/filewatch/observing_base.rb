# encoding: utf-8
require_relative 'bootstrap' unless defined?(FileWatch)

module FileWatch
  module ObservingBase
    attr_reader :watch, :sincedb_collection, :settings

    def initialize(opts={})
      options = {
        :sincedb_write_interval => 10,
        :stat_interval => 1,
        :discover_interval => 5,
        :exclude => [],
        :start_new_files_at => :end,
        :delimiter => "\n",
        :read_iterations => FIXNUM_MAX
      }.merge(opts)
      unless options.include?(:sincedb_path)
        options[:sincedb_path] = File.join(ENV["HOME"], ".sincedb") if ENV.include?("HOME")
        options[:sincedb_path] = ENV["SINCEDB_PATH"] if ENV.include?("SINCEDB_PATH")
      end
      unless options.include?(:sincedb_path)
        raise NoSinceDBPathGiven.new("No HOME or SINCEDB_PATH set in environment. I need one of these set so I can keep track of the files I am following.")
      end
      @settings = Settings.from_options(options)
      build_watch_and_dependencies
    end

    def build_watch_and_dependencies
      logger.info("START, creating Discoverer, Watch with file and sincedb collections")
      watched_files_collection = WatchedFilesCollection.new
      @sincedb_collection = SincedbCollection.new(@settings)
      @sincedb_collection.open
      discoverer = Discoverer.new(watched_files_collection, @sincedb_collection, @settings)
      @watch = Watch.new(discoverer, watched_files_collection, @settings)
      @watch.add_processor build_specific_processor(@settings)
    end

    def watch_this(path)
      @watch.watch(path)
    end

    def sincedb_write(reason=nil)
      # can be invoked from the file input
      @sincedb_collection.write(reason)
    end

    # quit is a sort-of finalizer,
    # it should be called for clean up
    # before the instance is disposed of.
    def quit
      logger.info("QUIT - closing all files and shutting down.")
      @watch.quit # <-- should close all the files
      # sincedb_write("shutting down")
    end

    # close_file(path) is to be used by external code
    # when it knows that it is completely done with a file.
    # Other files or folders may still be being watched.
    # Caution, once unwatched, a file can't be watched again
    # unless a new instance of this class begins watching again.
    # The sysadmin should rename, move or delete the file.
    def close_file(path)
      @watch.unwatch(path)
      sincedb_write
    end
  end
end
