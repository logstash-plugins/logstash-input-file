# encoding: utf-8

module FileWatch module ReadMode module Handlers
  class ReadFile < Base
    def handle_specifically(watched_file)
      if open_file(watched_file)
        add_or_update_sincedb_collection(watched_file) unless sincedb_collection.member?(watched_file.sincedb_key)
        @settings.file_chunk_count.times do
          begin
            data = watched_file.file_read(@settings.file_chunk_size)
            result = watched_file.buffer_extract(data) # expect BufferExtractResult
            logger.info(result.warning, result.additional) unless result.warning.empty?
            result.lines.each do |line|
              watched_file.listener.accept(line)
              # sincedb position is independent from the watched_file bytes_read
              sincedb_collection.increment(watched_file.sincedb_key, line.bytesize + @settings.delimiter_byte_size)
            end
            # instead of tracking the bytes_read line by line we need to track by the data read size.
            # because we initially seek to the bytes_read not the sincedb position
            watched_file.increment_bytes_read(data.bytesize)
            sincedb_collection.request_disk_flush
          rescue EOFError
            # flush the buffer now in case there is no final delimiter
            line = watched_file.buffer.flush
            watched_file.listener.accept(line) unless line.empty?
            watched_file.listener.eof
            watched_file.file_close
            # unset_watched_file will set sincedb_value.position to be watched_file.bytes_read
            sincedb_collection.unset_watched_file(watched_file)
            # we know we're done, we should not wait until the discovery of a
            # new file (or LS shutdown) to write out the sincedb
            sincedb_collection.immediate_write
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
      end
    end
  end
end end end
