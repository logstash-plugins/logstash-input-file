# encoding: utf-8

module FileWatch module ReadMode module Handlers
  class ReadFile < Base
    def handle_specifically(watched_file)
      if open_file(watched_file)
        add_or_update_sincedb_collection(watched_file) unless sincedb_collection.member?(watched_file.sincedb_key)
        changed = false
        logger.trace("reading...", "amount" => watched_file.read_bytesize_description, "filename" => watched_file.filename)
        watched_file.read_loop_count.times do
          break if quit?
          begin
            # expect BufferExtractResult
            result = watched_file.read_extract_lines
            # read_extract_lines will increment bytes_read
            logger.trace(result.warning, result.additional) unless result.warning.empty?
            changed = true
            result.lines.each do |line|
              watched_file.listener.accept(line)
              # sincedb position is independent from the watched_file bytes_read
              sincedb_collection.increment(watched_file.sincedb_key, line.bytesize + @settings.delimiter_byte_size)
            end
            sincedb_collection.request_disk_flush
          rescue EOFError
            # flush the buffer now in case there is no final delimiter
            line = watched_file.buffer.flush
            watched_file.listener.accept(line) unless line.empty?
            watched_file.listener.eof
            watched_file.file_close
            key = watched_file.sincedb_key
            sincedb_collection.reading_completed(key)
            sincedb_collection.clear_watched_file(key)
            watched_file.listener.deleted
            watched_file.unwatch
            break
          rescue Errno::EWOULDBLOCK, Errno::EINTR
            watched_file.listener.error
            break
          rescue => e
            logger.error("read_to_eof: general error reading file", "path" => watched_file.path, "error" => e.inspect, "backtrace" => e.backtrace.take(8))
            watched_file.listener.error
            break
          end
        end
      end
    end
  end
end end end
