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
        common_restat_with_delay(watched_file, "Closed") do
          if watched_file.size_changed?
            # if the closed file changed, move it to the watched state
            # not to active state because we want to respect the active files window.
            watched_file.watch
          end
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
        common_restat_with_delay(watched_file, "Ignored") do
          if watched_file.size_changed?
            watched_file.watch
            unignore(watched_file)
          end
        end
        break if watch.quit?
      end
    end

    def process_watched(watched_files)
      # Handles watched_files in the watched state.
      # for a slice of them:
      #   move to the active state
      #   and we allow the block to open the file and create a sincedb collection record if needed
      #   some have never been active and some have
      #   those that were active before but are watched now were closed under constraint
      #
      # defer the delete to one loop later to ensure that the stat really really can't find a renamed file
      # because a `stat` can be called right in the middle of the rotation rename cascade
      logger.debug("Delayed Delete processing")
      watched_files.select {|wf| wf.delayed_delete?}.each do |watched_file|
        common_restat_without_delay(watched_file, "Delayed Delete") do
          watched_file.restore_previous_state
          logger.debug("Delayed Delete: found previously unfound file: #{watched_file.path}")
        end
      end
      # do restat on all watched and active states once now. closed and ignored have been handled already
      logger.debug("Watched + Active processing")
      watched_files.select {|wf| wf.watched? || wf.active?}.each do |watched_file|
        common_restat_with_delay(watched_file, "Watched")
      end
      logger.debug("Watched processing")
      # how much of the max active window is available
      to_take = @settings.max_active - watched_files.count{|wf| wf.active?}
      if to_take > 0
        watched_files.select {|wf| wf.watched?}.take(to_take).each do |watched_file|
          watched_file.activate
          if watched_file.initial?
            create_initial(watched_file)
          else
            create(watched_file)
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
      # files have been opened at this point
      watched_files.select {|wf| wf.active? }.each do |watched_file|
        break if watch.quit?
        path = watched_file.path
        logger.debug("Active - info",
          "sincedb_key" => watched_file.sincedb_key,
          "size" => watched_file.last_stat_size,
          "previous size" => watched_file.previous_inode_size,
          "active size" => watched_file.active_stat_size,
          "pending_inode_count" => watched_file.pending_inode_count,
          "read" => watched_file.bytes_read,
          "unread" => watched_file.bytes_unread,
          "path" => path)
        # when the file is open, all size based tests are driven from the open file not the path
        if watched_file.new_inode_detected?
          if watched_file.all_previous_bytes_read?
            # rotated with a new inode and is fully read
            # close file but keep buffer contents if any
            watched_file.file_close
            # now we can use the path based stat for the sincedb_key
            # and reset the pending_inode_count
            watched_file.set_sincedb_key_from_path_based_stat
            # put it back to watched
            watched_file.watch
            logger.debug("Active - inode change detected, closing old file handle & set back to watched: #{path}")
          else
            # rotated file but original opened file is not fully read
            # we need to keep reading the open file, if we close it we lose it because the path is now pointing at a different file.
            logger.debug("Active - inode change detected and not fully read: new size is #{watched_file.last_stat_size}, old size #{watched_file.previous_inode_size}: #{path}")
            # need to fully read open file while we can
            watched_file.set_depth_first_read_loop
            grow(watched_file)
            watched_file.file_close
            watched_file.set_sincedb_key_from_path_based_stat
            watched_file.set_user_defined_read_loop
            watched_file.watch
            logger.debug("Active - inode change detected, closing old file handle & set back to watched: #{path}")
          end
        else
          if watched_file.grown?
            logger.debug("Active - file grew: #{path}: new size is #{watched_file.last_stat_size}, bytes read #{watched_file.bytes_read}")
            grow(watched_file)
          elsif watched_file.shrunk?
            if watched_file.bytes_unread > 0
              logger.warn("Active - shrunk: DATA LOSS!! truncate detected with #{watched_file.bytes_unread} unread bytes: #{path}")
            end
            # we don't update the size here, its updated when we actually read
            logger.debug("Active - file shrunk #{path}: new size is #{watched_file.last_stat_size}, old size #{watched_file.bytes_read}")
            shrink(watched_file)
          else
            # same size, do nothing
          end
        end
        # can any active files be closed to make way for waiting files?
        if watched_file.file_closable?
          logger.debug("Watch each: active: file expired: #{path}")
          timeout(watched_file)
          watched_file.close
        end
      end
    end

    def common_restat_with_delay(watched_file, action, &block)
      common_restat(watched_file, action, true, &block)
    end

    def common_restat_without_delay(watched_file, action, &block)
      common_restat(watched_file, action, false, &block)
    end

    def common_restat(watched_file, action, delay, &block)
      all_ok = true
      begin
        watched_file.restat
        yield if block_given?
      rescue Errno::ENOENT
        if delay
          logger.debug("#{action} - delaying the stat fail on: #{watched_file.path}")
          watched_file.delay_delete
        else
          # file has gone away or we can't read it anymore.
          logger.debug("#{action} - really can't find this file: #{watched_file.path}")
          watched_file.unwatch
          logger.debug("#{action} - removing from collection: #{watched_file.path}")
          delete(watched_file)
          deletable_filepaths << watched_file.path
          all_ok = false
        end
      rescue => e
        logger.error("#{action} - other error #{path}: (#{error.message}, #{error.backtrace.take(8).inspect})")
        all_ok = false
      end
      all_ok
    end
  end
end end
