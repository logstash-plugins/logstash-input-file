# encoding: utf-8

module FileWatch module ReadMode module Handlers
  class ReadFile < Base
    def handle_specifically(watched_file)
      if open_file(watched_file)
        add_or_update_sincedb_collection(watched_file) unless sincedb_collection.member?(watched_file.sincedb_key)
        changed = false
        @settings.file_chunk_count.times do
          begin
            data = watched_file.file_read(@settings.file_chunk_size)
            lines = watched_file.buffer_extract(data)
            if lines.empty?
              log_delimiter_not_found(watched_file, data.bytesize)
            end
            changed = true
            lines.each do |line|
              watched_file.listener.accept(line)
              # sincedb position is independent from the watched_file bytes_read
              sincedb_collection.increment(watched_file.sincedb_key, line.bytesize + @settings.delimiter_byte_size)
            end
            # instead of tracking the bytes_read line by line we need to track by the data read size.
            # because we initially seek to the bytes_read not the sincedb position
            watched_file.increment_bytes_read(data.bytesize)
          rescue EOFError
            # flush the buffer now in case there is no final delimiter
            line = watched_file.buffer.flush
            watched_file.listener.accept(line) unless line.empty?
            watched_file.listener.eof
            watched_file.file_close
            # unset_watched_file will set sincedb_value.position to be watched_file.bytes_read
            sincedb_collection.unset_watched_file(watched_file)
            watched_file.listener.deleted
            watched_file.unwatch
            break
          rescue Errno::EWOULDBLOCK, Errno::EINTR
            watched_file.listener.error
            break
          rescue => e
            logger.error("read_to_eof: general error reading #{watched_file.path} - error: #{e.inspect}")
            watched_file.listener.error
            break
          end
        end
        sincedb_collection.request_disk_flush if changed
      end
    end

    private

    def log_delimiter_not_found(watched_file, data_size)
      warning = "read_to_eof: a delimiter can't be found in current chunk"
      warning.concat(", maybe there are no more delimiters or the delimiter is incorrect")
      warning.concat(" or the text before the delimiter, a 'line', is very large")
      warning.concat(", if this message is logged often try increasing the `file_chunk_size` setting.")
      log_details = {
        "delimiter" => @settings.delimiter,
        "read_position" => watched_file.bytes_read,
        "bytes_read_count" => data_size,
        "last_known_file_size" => watched_file.last_stat_size,
        "file_path" => watched_file.path,
      }
      logger.info(warning, log_details)
    end
  end
end end end
