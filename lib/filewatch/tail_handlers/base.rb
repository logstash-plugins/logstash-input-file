# encoding: utf-8
#
require "logstash/util/loggable"
module FileWatch module TailHandlers
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
      # STDERR.puts "-------------------------------- >> #{self.class.name} handle - state history is #{watched_file.full_state_history.inspect}"
      handle_specifically(watched_file)
    end

    def handle_specifically(watched_file)
      # some handlers don't need to define this method
    end

    def update_existing_specifically(watched_file, sincedb_value)
      # when a handler subclass does not implement this then do nothing
    end

    private

    def read_to_eof(watched_file)
      changed = false
      FIXNUM_MAX.times do
        begin
          data = watched_file.file_read(OPTS.file_chunk_size)
          lines = watched_file.buffer_extract(data)
          logger.warn("read_to_eof: no delimiter found in current chunk") if lines.empty?
          changed = true
          lines.each do |line|
            watched_file.listener.accept(line)
            sincedb_collection.increment(watched_file.sincedb_key, line.bytesize + OPTS.delimiter_byte_size)
          end
        rescue EOFError
          # it only makes sense to signal EOF in "read" mode not "tail"
          break
        rescue Errno::EWOULDBLOCK, Errno::EINTR
          watched_file.listener.error
          break
        rescue => e
          logger.error("read_to_eof: general error reading #{watched_file.path}", "error" => e.inspect, "backtrace" => e.backtrace.take(4))
          watched_file.listener.error
          break
        end
      end
      sincedb_collection.request_disk_flush if changed
    end

    def open_file(watched_file)
      return true if watched_file.file_open?
      logger.debug("TailHandlers::Handler - opening #{watched_file.path}")
      begin
        watched_file.open
      rescue
        # don't emit this message too often. if a file that we can't
        # read is changing a lot, we'll try to open it more often, and spam the logs.
        now = Time.now.to_i
        logger.warn("open_file OPEN_WARN_INTERVAL is '#{OPEN_WARN_INTERVAL}'")
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
        # STDERR.puts "-------------------------------- >> handle - opened file"
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
      update_existing_specifically(watched_file, sincedb_value)
    end

    def add_new_value_sincedb_collection(watched_file)
      sincedb_value = get_new_value_specifically(watched_file)
      logger.debug("add_new_value_sincedb_collection: #{watched_file.path}", "position" => sincedb_value.position)
      sincedb_collection.set(watched_file.sincedb_key, sincedb_value)
    end

    def get_new_value_specifically(watched_file)
      position = OPTS.start_new_files_at == :beginning ? 0 : watched_file.last_stat_size
      value = SincedbValue.new(position)
      value.set_watched_file(watched_file)
      watched_file.update_bytes_read(position)
      value
    end
  end
end end

require_relative "dispatch"
