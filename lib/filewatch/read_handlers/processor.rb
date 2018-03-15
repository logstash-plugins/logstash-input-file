# encoding: utf-8
require "logstash/util/loggable"
module FileWatch module ReadHandlers
  class Processor
    include LogStash::Util::Loggable

    attr_reader :watch, :deletable_filepaths

    def initialize
      @deletable_filepaths = []
    end

    def add_watch(watch)
      @watch = watch
      self
    end

    def process_closed(watched_files, dispatcher)
      # do not process watched_files in the closed state.
    end

    def process_ignored(watched_files, dispatcher)
      # do not process watched_files in the closed state.
    end

    def process_watched(watched_files, dispatcher)
      logger.debug("Watched processing")
      # Handles watched_files in the watched state.
      # for a slice of them:
      #   move to the active state
      #   should never have been active before
      # how much of the max active window is available
      to_take = OPTS.max_active - watched_files.count{|wf| wf.active?}
      if to_take > 0
        watched_files.select {|wf| wf.watched?}.take(to_take).each do |watched_file|
          path = watched_file.path
          begin
            watched_file.restat
            watched_file.activate
          rescue Errno::ENOENT
            common_deleted_reaction(watched_file, dispatcher, "Watched")
            next
          rescue => e
            common_error_reaction(path, e, "Watched")
            next
          end
          break if watch.quit?
        end
      else
        now = Time.now.to_i
        if (now - watch.lastwarn_max_files) > MAX_FILES_WARN_INTERVAL
          waiting = watched_files.size - OPTS.max_active
          logger.warn(OPTS.max_warn_msg + ", files yet to open: #{waiting}")
          watch.lastwarn_max_files = now
        end
      end
    end

    def process_active(watched_files, dispatcher)
      logger.debug("Active processing")
      # Handles watched_files in the active state.
      watched_files.select {|wf| wf.active? }.each do |watched_file|
        path = watched_file.path
        begin
          watched_file.restat
        rescue Errno::ENOENT
          common_deleted_reaction(watched_file, dispatcher, "Active")
          next
        rescue => e
          common_error_reaction(path, e, "Active")
          next
        end
        break if watch.quit?

        if watched_file.compressed?
          dispatcher.read_zip_file(watched_file)
        else
          dispatcher.read_file(watched_file)
        end
        # dispatched handlers take care of closing and unwatching
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
