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
      @sincedb_collection = sincedb_collection
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
      # logger.trace("Closed processing")
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
      # logger.trace("Ignored processing")
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
      logger.trace("Delayed Delete processing")
      watched_files.select {|wf| wf.delayed_delete?}.each do |watched_file|
        logger.trace(">>> Delayed Delete", "path" => watched_file.filename)
        common_restat_without_delay(watched_file, ">>> Delayed Delete") do
          logger.trace(">>> Delayed Delete: file at path found again", "watched_file" => watched_file.details)
          watched_file.file_at_path_found_again
        end
      end
      # do restat on all watched and active states once now. closed and ignored have been handled already
      logger.trace("Watched + Active processing")
      watched_files.select {|wf| wf.watched? || wf.active?}.each do |watched_file|
        common_restat_with_delay(watched_file, "Watched")
      end
      rotation_set = watched_files.select {|wf| wf.rotation_in_progress?}
      if !rotation_set.empty?
        logger.trace(">>> Rotation In Progress ....")
        rotation_set.each do |watched_file|
          # log each for now
          sdb_value = @sincedb_collection.find(watched_file)
          potential_key = watched_file.path_based_sincedb_key
          potential_sdb_value =  @sincedb_collection.get(potential_key)
          logger.trace(">>> Rotation In Progress", "watched_file" => watched_file.details, "found_sdb_value" => sdb_value, "potential_key" => potential_key, "potential_sdb_value" => potential_sdb_value)
          if potential_sdb_value.nil?
            if sdb_value.nil?
              logger.trace("---------- >>> Rotation In Progress: rotating as new file, no potential sincedb value AND no found sincedb value")
              watched_file.rotate_as_initial_file
            else
              logger.trace("---------- >>>> Rotation In Progress: rotating as existing file, no potential sincedb value BUT found sincedb value")
              sdb_value.clear_watched_file
              watched_file.rotate_as_file
            end
          else
            other_watched_file = potential_sdb_value.watched_file
            potential_sdb_value.set_watched_file(watched_file)
            if other_watched_file.nil?
              logger.trace("---------- >>>> Rotation In Progress: rotating as existing file WITH potential sincedb value")
              watched_file.rotate_as_file
              watched_file.update_bytes_read(potential_sdb_value.position)
            else
              logger.trace("---------- >>>> Rotation In Progress: rotating from...", "this watched_file details" => watched_file.details, "other watched_file details" => other_watched_file.details)
              watched_file.rotate_from(other_watched_file)
            end
          end
        end
      end
      logger.trace("Watched processing")
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
      # logger.trace("Active processing")
      # Handles watched_files in the active state.
      # files have been opened at this point
      watched_files.select {|wf| wf.active? }.each do |watched_file|
        break if watch.quit?
        path = watched_file.filename
        logger.trace("Active - info",
          "sincedb_key" => watched_file.sincedb_key,
          "size" => watched_file.last_stat_size,
          "active size" => watched_file.active_stat_size,
          "read" => watched_file.bytes_read,
          "unread" => watched_file.bytes_unread,
          "filename" => path)
        # when the file is open, all size based tests are driven from the open file not the path
        if watched_file.rotation_detected?
          # rotated with a new inode and is fully read
          # keep buffer contents if any
          logger.trace(">>> Active - inode change detected, set to rotation_in_progress", "path" => path)
          watched_file.rotation_in_progress
          if watched_file.all_open_file_bytes_read?
            logger.trace(">>> Active - inode change detected and file is fully read")
          else
            # rotated file but original opened file is not fully read
            # we need to keep reading the open file, if we close it we lose it because the path is now pointing at a different file.
            logger.trace(">>> Active - inode change detected and file is not fully read", "watched_file details" => watched_file.details)
            # need to fully read open file while we can
            watched_file.set_depth_first_read_loop
            grow(watched_file)
            watched_file.set_user_defined_read_loop
          end
        else
          if watched_file.grown?
            logger.trace("Active - file grew: #{path}: new size is #{watched_file.last_stat_size}, bytes read #{watched_file.bytes_read}")
            grow(watched_file)
          elsif watched_file.shrunk?
            if watched_file.bytes_unread > 0
              logger.warn("Active - shrunk: DATA LOSS!! truncate detected with #{watched_file.bytes_unread} unread bytes: #{path}")
            end
            # we don't update the size here, its updated when we actually read
            logger.trace("Active - file shrunk #{path}: new size is #{watched_file.last_stat_size}, old size #{watched_file.bytes_read}")
            shrink(watched_file)
          else
            # same size, do nothing
          end
        end
        # can any active files be closed to make way for waiting files?
        if watched_file.file_closable?
          logger.trace("Watch each: active: file expired: #{path}")
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
          logger.trace("#{action} - delaying the stat fail on: #{watched_file.filename}")
          watched_file.delay_delete
        else
          # file has gone away or we can't read it anymore.
          logger.trace("#{action} - after a delay, really can't find this file: #{watched_file.filename}")
          watched_file.unwatch
          logger.trace("#{action} - removing from collection: #{watched_file.filename}")
          delete(watched_file)
          deletable_filepaths << watched_file.path
          all_ok = false
        end
      rescue => e
        logger.error("#{action} - other error #{watched_file.path}: (#{e.message}, #{e.backtrace.take(8).inspect})")
        all_ok = false
      end
      all_ok
    end
  end
end end
