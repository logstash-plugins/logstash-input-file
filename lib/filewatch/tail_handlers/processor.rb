# encoding: utf-8
require "logstash/util/loggable"

module FileWatch module TailHandlers
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

    def process_closed(watched_files, dispatcher)
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
          common_deleted_reaction(watched_file, dispatcher, "Closed")
        rescue => e
          common_error_reaction(path, e, "Closed")
        end
        break if watch.quit?
      end
    end

    def process_ignored(watched_files, dispatcher)
      logger.debug("Ignored processing")
      # Handles watched_files in the ignored state.
      # if its size changed:
      #   put it in the watched state
      #   invoke unignore on the handler
      watched_files.select {|wf| wf.ignored? }.each do |watched_file|
        path = watched_file.path
        begin
          watched_file.restat
          if watched_file.size_changed?
            watched_file.watch
            dispatcher.unignore(watched_file)
          end
        rescue Errno::ENOENT
          # file has gone away or we can't read it anymore.
          common_deleted_reaction(watched_file, dispatcher, "Ignored")
        rescue => e
          common_error_reaction(path, e, "Ignored")
        end
        break if watch.quit?
      end
    end

    def process_watched(watched_files, dispatcher)
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
              dispatcher.create_initial(watched_file)
            else
              dispatcher.create(watched_file)
            end
          rescue Errno::ENOENT
            # file has gone away or we can't read it anymore.
            common_deleted_reaction(watched_file, dispatcher, "Watched")
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

    def process_active(watched_files, dispatcher)
      logger.debug("Active processing")
      # Handles watched_files in the active state.
      # it has been read once - unless they were empty at the time
      watched_files.select {|wf| wf.active? }.each do |watched_file|
        path = watched_file.path
        begin
          watched_file.restat
        rescue Errno::ENOENT
          # file has gone away or we can't read it anymore.
          common_deleted_reaction(watched_file, dispatcher, "Active")
          next
        rescue => e
          common_error_reaction(path, e, "Active")
          next
        end
        break if watch.quit?

        shrinking = watched_file.shrunk?
        growing = watched_file.grown?

        if growing
          logger.debug("Active - file grew: #{path}: new size is #{watched_file.last_stat_size}, old size #{watched_file.bytes_read}")
          dispatcher.grow(watched_file)
        elsif shrinking
          # we don't update the size here, its updated when we actually read
          logger.debug("Active - file shrunk #{path}: new size is #{watched_file.last_stat_size}, old size #{watched_file.bytes_read}")
          dispatcher.shrink(watched_file)
        else
          # same size, do nothing
        end
        # can any active files be closed to make way for waiting files?
        if watched_file.file_closable?
          logger.debug("Watch each: active: file expired: #{path}")
          dispatcher.timeout(watched_file)
          watched_file.close
        end
      end
    end

    def common_deleted_reaction(watched_file, dispatcher, action)
      # file has gone away or we can't read it anymore.
      watched_file.unwatch
      dispatcher.delete(watched_file)
      deletable_filepaths << watched_file.path
      logger.debug("#{action} - stat failed: #{watched_file.path}, removing from collection")
    end

    def common_error_reaction(path, error, action)
      logger.error("#{action} - other error #{path}: (#{error.message}, #{error.backtrace.take(8).inspect})")
    end
  end
end end
