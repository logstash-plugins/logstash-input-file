# encoding: utf-8
require "logstash/util/loggable"

module FileWatch module TailMode module Handlers
  class Base
    include LogStash::Util::Loggable
    attr_reader :sincedb_collection

    def initialize(processor, sincedb_collection, observer, settings)
      @settings = settings
      @processor = processor
      @sincedb_collection = sincedb_collection
      @observer = observer
    end

    def quit?
      @processor.watch.quit?
    end

    def handle(watched_file)
      logger.trace? && logger.trace("handling:", :path => watched_file.path)
      unless watched_file.has_listener?
        watched_file.set_listener(@observer)
      end
      handle_specifically(watched_file)
    end

    def handle_specifically(watched_file)
      # some handlers don't need to define this method
    end

    def update_existing_specifically(watched_file, sincedb_value)
      # when a handler subclass does not implement this then do nothing
    end

    private

    def controlled_read(watched_file, loop_control)
      changed = false
      logger.trace? && logger.trace(__method__.to_s, :iterations => loop_control.count, :amount => loop_control.size, :filename => watched_file.filename)
      # from a real config (has 102 file inputs)
      # -- This cfg creates a file input for every log file to create a dedicated file pointer and read all file simultaneously
      # -- If we put all log files in one file input glob we will have indexing delay, because Logstash waits until the first file becomes EOF
      # by allowing the user to specify a combo of `file_chunk_count` X `file_chunk_size`...
      # we enable the pseudo parallel processing of each file.
      # user also has the option to specify a low `stat_interval` and a very high `discover_interval`to respond
      # quicker to changing files and not allowing too much content to build up before reading it.
      loop_control.count.times do
        break if quit?
        begin
          logger.debug? && logger.debug("#{__method__} get chunk")
          result = watched_file.read_extract_lines(loop_control.size) # expect BufferExtractResult
          logger.trace(result.warning, result.additional) unless result.warning.empty?
          changed = true
          result.lines.each do |line|
            watched_file.listener.accept(line)
            # sincedb position is now independent from the watched_file bytes_read
            sincedb_collection.increment(watched_file.sincedb_key, line.bytesize + @settings.delimiter_byte_size)
          end
        rescue EOFError => e
          # it only makes sense to signal EOF in "read" mode not "tail"
          logger.debug(__method__.to_s, exception_details(watched_file.path, e, false))
          loop_control.flag_read_error
          break
        rescue Errno::EWOULDBLOCK, Errno::EINTR => e
          logger.debug(__method__.to_s, exception_details(watched_file.path, e, false))
          watched_file.listener.error
          loop_control.flag_read_error
          break
        rescue => e
          logger.error("#{__method__} general error reading", exception_details(watched_file.path, e))
          watched_file.listener.error
          loop_control.flag_read_error
          break
        end
      end
      logger.debug("#{__method__} stopped loop due quit") if quit?
      sincedb_collection.request_disk_flush if changed
    end

    def open_file(watched_file)
      return true if watched_file.file_open?
      logger.trace? && logger.trace("open_file", :filename => watched_file.filename)
      begin
        watched_file.open
      rescue => e
        # don't emit this message too often. if a file that we can't
        # read is changing a lot, we'll try to open it more often, and spam the logs.
        now = Time.now.to_i
        logger.trace? && logger.trace("open_file OPEN_WARN_INTERVAL is '#{OPEN_WARN_INTERVAL}'")
        if watched_file.last_open_warning_at.nil? || now - watched_file.last_open_warning_at > OPEN_WARN_INTERVAL
          logger.warn("failed to open file", exception_details(watched_file.path, e))
          watched_file.last_open_warning_at = now
        else
          logger.debug("open_file suppressed warning `failed to open file`", exception_details(watched_file.path, e, false))
        end
        watched_file.watch # set it back to watch so we can try it again
      else
        watched_file.listener.opened
      end
      watched_file.file_open?
    end

    def add_or_update_sincedb_collection(watched_file)
      sincedb_value = @sincedb_collection.find(watched_file)
      if sincedb_value.nil?
        sincedb_value = add_new_value_sincedb_collection(watched_file)
        watched_file.initial_completed
      elsif sincedb_value.watched_file == watched_file
        update_existing_sincedb_collection_value(watched_file, sincedb_value)
        watched_file.initial_completed
      else
        logger.trace? && logger.trace("add_or_update_sincedb_collection: found sincedb record",
                                      :sincedb_key => watched_file.sincedb_key, :sincedb_value => sincedb_value)
        # detected a rotation, Discoverer can't handle this because this watched file is not a new discovery.
        # we must handle it here, by transferring state and have the sincedb value track this watched file
        # rotate_as_file and rotate_from will switch the sincedb key to the inode that the path is now pointing to
        # and pickup the sincedb_value from before.
        logger.debug("add_or_update_sincedb_collection: the found sincedb_value has a watched_file - this is a rename, switching inode to this watched file")
        existing_watched_file = sincedb_value.watched_file
        if existing_watched_file.nil?
          sincedb_value.set_watched_file(watched_file)
          logger.trace? && logger.trace("add_or_update_sincedb_collection: switching as new file")
          watched_file.rotate_as_file
          watched_file.update_bytes_read(sincedb_value.position)
        else
          sincedb_value.set_watched_file(watched_file)
          logger.trace? && logger.trace("add_or_update_sincedb_collection: switching from:", :watched_file => watched_file.details)
          watched_file.rotate_from(existing_watched_file)
        end
      end
      sincedb_value
    end

    def update_existing_sincedb_collection_value(watched_file, sincedb_value)
      logger.trace? && logger.trace("update_existing_sincedb_collection_value", :position => sincedb_value.position,
                                    :filename => watched_file.filename, :last_stat_size => watched_file.last_stat_size)
      update_existing_specifically(watched_file, sincedb_value)
    end

    def add_new_value_sincedb_collection(watched_file)
      sincedb_value = get_new_value_specifically(watched_file)
      logger.trace? && logger.trace("add_new_value_sincedb_collection", :position => sincedb_value.position,
                                    :watched_file => watched_file.details)
      sincedb_collection.set(watched_file.sincedb_key, sincedb_value)
      sincedb_value
    end

    def get_new_value_specifically(watched_file)
      position = watched_file.position_for_new_sincedb_value
      value = SincedbValue.new(position)
      value.set_watched_file(watched_file)
      watched_file.update_bytes_read(position)
      value
    end

    private

    def exception_details(path, e, trace = true)
      details = { :path => path, :exception => e.class, :message => e.message }
      details[:backtrace] = e.backtrace if trace && logger.debug?
      details
    end

  end
end end end
