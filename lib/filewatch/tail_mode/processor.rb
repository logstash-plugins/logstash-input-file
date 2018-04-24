# encoding: utf-8
require "logstash/util/loggable"
require_relative "handlers/base"
require_relative "handlers/create_initial"
require_relative "handlers/create"
require_relative "handlers/delete"
require_relative "handlers/grow"
require_relative "handlers/shrink"
require_relative "handlers/timeout"
require_relative "handlers/unignore"

module FileWatch module TailMode
  # Must handle
  #   :create_initial - file is discovered and we have no record of it in the sincedb
  #   :create - file is discovered and we have seen it before in the sincedb
  #   :grow   - file has more content
  #   :shrink - file has less content
  #   :delete   - file can't be read
  #   :timeout - file is closable
  #   :unignore - file was ignored, but have now received new content
  class Processor
    include LogStash::Util::Loggable

    attr_reader :watch, :deletable_filepaths

    def initialize(settings)
      @settings = settings
      @deletable_filepaths = []
    end

    def add_watch(watch)
      @watch = watch
      self
    end

    def initialize_handlers(sincedb_collection, observer)
      @create_initial = Handlers::CreateInitial.new(sincedb_collection, observer, @settings)
      @create = Handlers::Create.new(sincedb_collection, observer, @settings)
      @grow = Handlers::Grow.new(sincedb_collection, observer, @settings)
      @shrink = Handlers::Shrink.new(sincedb_collection, observer, @settings)
      @delete = Handlers::Delete.new(sincedb_collection, observer, @settings)
      @timeout = Handlers::Timeout.new(sincedb_collection, observer, @settings)
      @unignore = Handlers::Unignore.new(sincedb_collection, observer, @settings)
    end

    def create(watched_file)
      @create.handle(watched_file)
    end

    def create_initial(watched_file)
      @create_initial.handle(watched_file)
    end

    def grow(watched_file)
      @grow.handle(watched_file)
    end

    def shrink(watched_file)
      @shrink.handle(watched_file)
    end

    def delete(watched_file)
      @delete.handle(watched_file)
    end

    def timeout(watched_file)
      @timeout.handle(watched_file)
    end

    def unignore(watched_file)
      @unignore.handle(watched_file)
    end

    def process_closed(watched_files)
      logger.debug("Closed processing")
      # Handles watched_files in the closed state.
      # if its size changed it is put into the watched state
      watched_files.select {|wf| wf.closed? }.each do |watched_file|
        path = watched_file.path
        begin
          watched_file.restat
          if watched_file.size_changed?
            # if the closed file changed, move it to the watched state
            # not to active state because we want to respect the active files window.
            watched_file.watch
          end
        rescue Errno::ENOENT
          # file has gone away or we can't read it anymore.
          common_deleted_reaction(watched_file, "Closed")
        rescue => e
          common_error_reaction(path, e, "Closed")
        end
        break if watch.quit?
      end
    end

    def process_ignored(watched_files)
      logger.debug("Ignored processing")
      # Handles watched_files in the ignored state.
      # if its size changed:
      #   put it in the watched state
      #   invoke unignore
      watched_files.select {|wf| wf.ignored? }.each do |watched_file|
        path = watched_file.path
        begin
          watched_file.restat
          if watched_file.size_changed?
            watched_file.watch
            unignore(watched_file)
          end
        rescue Errno::ENOENT
          # file has gone away or we can't read it anymore.
          common_deleted_reaction(watched_file, "Ignored")
        rescue => e
          common_error_reaction(path, e, "Ignored")
        end
        break if watch.quit?
      end
    end

    def process_watched(watched_files)
      logger.debug("Watched processing")
      # Handles watched_files in the watched state.
      # for a slice of them:
      #   move to the active state
      #   and we allow the block to open the file and create a sincedb collection record if needed
      #   some have never been active and some have
      #   those that were active before but are watched now were closed under constraint

      # how much of the max active window is available
      to_take = @settings.max_active - watched_files.count{|wf| wf.active?}
      if to_take > 0
        watched_files.select {|wf| wf.watched?}.take(to_take).each do |watched_file|
          path = watched_file.path
          begin
            watched_file.restat
            watched_file.activate
            if watched_file.initial?
              create_initial(watched_file)
            else
              create(watched_file)
            end
          rescue Errno::ENOENT
            # file has gone away or we can't read it anymore.
            common_deleted_reaction(watched_file, "Watched")
          rescue => e
            common_error_reaction(path, e, "Watched")
          end
          break if watch.quit?
        end
      else
        now = Time.now.to_i
        if (now - watch.lastwarn_max_files) > MAX_FILES_WARN_INTERVAL
          waiting = watched_files.size - @settings.max_active
          logger.warn(@settings.max_warn_msg + ", files yet to open: #{waiting}")
          watch.lastwarn_max_files = now
        end
      end
    end

    def process_active(watched_files)
      logger.debug("Active processing")
      # Handles watched_files in the active state.
      # it has been read once - unless they were empty at the time
      watched_files.select {|wf| wf.active? }.each do |watched_file|
        path = watched_file.path
        begin
          watched_file.restat
        rescue Errno::ENOENT
          # file has gone away or we can't read it anymore.
          common_deleted_reaction(watched_file, "Active")
          next
        rescue => e
          common_error_reaction(path, e, "Active")
          next
        end
        break if watch.quit?
        if watched_file.grown?
          logger.debug("Active - file grew: #{path}: new size is #{watched_file.last_stat_size}, old size #{watched_file.bytes_read}")
          grow(watched_file)
        elsif watched_file.shrunk?
          # we don't update the size here, its updated when we actually read
          logger.debug("Active - file shrunk #{path}: new size is #{watched_file.last_stat_size}, old size #{watched_file.bytes_read}")
          shrink(watched_file)
        else
          # same size, do nothing
        end
        # can any active files be closed to make way for waiting files?
        if watched_file.file_closable?
          logger.debug("Watch each: active: file expired: #{path}")
          timeout(watched_file)
          watched_file.close
        end
      end
    end

    def common_deleted_reaction(watched_file, action)
      # file has gone away or we can't read it anymore.
      watched_file.unwatch
      delete(watched_file)
      deletable_filepaths << watched_file.path
      logger.debug("#{action} - stat failed: #{watched_file.path}, removing from collection")
    end

    def common_error_reaction(path, error, action)
      logger.error("#{action} - other error #{path}: (#{error.message}, #{error.backtrace.take(8).inspect})")
    end
  end
end end
