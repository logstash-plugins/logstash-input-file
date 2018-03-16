# encoding: utf-8
require "logstash/util/loggable"
module FileWatch module ReadHandlers
  class Base
    include LogStash::Util::Loggable

    attr_reader :sincedb_collection

    def initialize(sincedb_collection, observer)
      @sincedb_collection = sincedb_collection
      @observer = observer
    end

    def handle(watched_file)
      logger.debug("handling: #{watched_file.path}")
      unless watched_file.has_listener?
        watched_file.set_listener(@observer)
      end
      handle_specifically(watched_file)
    end

    def handle_specifically(watched_file)
      # some handlers don't need to define this method
    end

    private

    def open_file(watched_file)
      return true if watched_file.file_open?
      logger.debug("opening #{watched_file.path}")
      begin
        watched_file.open
      rescue
        # don't emit this message too often. if a file that we can't
        # read is changing a lot, we'll try to open it more often, and spam the logs.
        now = Time.now.to_i
        logger.warn("opening OPEN_WARN_INTERVAL is '#{OPEN_WARN_INTERVAL}'")
        if watched_file.last_open_warning_at.nil? || now - watched_file.last_open_warning_at > OPEN_WARN_INTERVAL
          logger.warn("failed to open #{watched_file.path}: #{$!.inspect}, #{$!.backtrace.take(3)}")
          watched_file.last_open_warning_at = now
        else
          logger.debug("suppressed warning for `failed to open` #{watched_file.path}: #{$!.inspect}")
        end
        watched_file.watch # set it back to watch so we can try it again
      end
      if watched_file.file_open?
        watched_file.listener.opened
        true
      else
        false
      end
    end

    def add_or_update_sincedb_collection(watched_file)
      sincedb_value = @sincedb_collection.find(watched_file)
      if sincedb_value.nil?
        add_new_value_sincedb_collection(watched_file)
      elsif sincedb_value.watched_file == watched_file
        update_existing_sincedb_collection_value(watched_file, sincedb_value)
      else
        logger.warn? && logger.warn("mismatch on sincedb_value.watched_file, this should have been handled by Discoverer")
      end
      watched_file.initial_completed
    end

    def update_existing_sincedb_collection_value(watched_file, sincedb_value)
      logger.debug("update_existing_sincedb_collection_value: #{watched_file.path}, last value #{sincedb_value.position}, cur size #{watched_file.last_stat_size}")
      # sincedb_value is the source of truth
      watched_file.update_bytes_read(sincedb_value.position)
    end

    def add_new_value_sincedb_collection(watched_file)
      sincedb_value = SincedbValue.new(0)
      sincedb_value.set_watched_file(watched_file)
      logger.debug("add_new_value_sincedb_collection: #{watched_file.path}", "position" => sincedb_value.position)
      sincedb_collection.set(watched_file.sincedb_key, sincedb_value)
    end
  end
end end

require_relative "dispatch"
