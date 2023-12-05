# encoding: utf-8

module FileWatch module ReadMode module Handlers
  class ReadFile < Base

    # seek file to which ever is furthest: either current bytes read or sincedb position
    private
    def seek_to_furthest_position(watched_file)
      previous_pos = sincedb_collection.find(watched_file).position
      watched_file.file_seek([watched_file.bytes_read, previous_pos].max)
    end

    public
    def handle_specifically(watched_file)
      if open_file(watched_file)
        add_or_update_sincedb_collection(watched_file) unless sincedb_collection.member?(watched_file.sincedb_key)
        seek_to_furthest_position(watched_file)
        loop do
          break if quit?
          loop_control = watched_file.loop_control_adjusted_for_stat_size
          controlled_read(watched_file, loop_control)
          sincedb_collection.request_disk_flush
          break unless loop_control.keep_looping?
        end
        if watched_file.all_read?
          # flush the buffer now in case there is no final delimiter
          line = watched_file.buffer.flush
          watched_file.listener.accept(line) unless line.empty?
          watched_file.listener.eof
          watched_file.file_close
          key = watched_file.sincedb_key
          if sincedb_collection.get(key)
            sincedb_collection.reading_completed(key)
            sincedb_collection.clear_watched_file(key)
          end
          watched_file.listener.deleted
          # NOTE: on top of un-watching we should also remove from the watched files collection
          # if the file is getting deleted (on completion), that part currently resides in
          # DeleteCompletedFileHandler - triggered above using `watched_file.listener.deleted`
          watched_file.unwatch
        end
      end
    end

    def controlled_read(watched_file, loop_control)
      logger.trace? && logger.trace("reading...", :filename => watched_file.filename, :iterations => loop_control.count, :amount => loop_control.size)
      loop_control.count.times do
        break if quit?
        begin
          result = watched_file.read_extract_lines(loop_control.size) # expect BufferExtractResult
          logger.info(result.warning, result.additional) unless result.warning.empty?
          result.lines.each do |line|
            watched_file.listener.accept(line)
            # sincedb position is independent from the watched_file bytes_read
            delta = line.bytesize + @settings.delimiter_byte_size
            sincedb_collection.increment(watched_file.sincedb_key, delta)
            break if quit?
          end
        rescue EOFError => e
          log_error("controlled_read: eof error reading file", watched_file, e)
          loop_control.flag_read_error
          break
        rescue Errno::EWOULDBLOCK, Errno::EINTR => e
          log_error("controlled_read: block or interrupt error reading file", watched_file, e)
          watched_file.listener.error
          loop_control.flag_read_error
          break
        rescue => e
          log_error("controlled_read: general error reading file", watched_file, e)
          watched_file.listener.error
          loop_control.flag_read_error
          break
        end
      end
    end

    def log_error(msg, watched_file, error)
      details = { :path => watched_file.path,
                  :exception => error.class,
                  :message => error.message,
                  :backtrace => error.backtrace }
      if logger.debug?
        details[:file] = watched_file
      else
        details[:backtrace] = details[:backtrace].take(8) if details[:backtrace]
      end
      logger.error(msg, details)
    end
  end
end end end
