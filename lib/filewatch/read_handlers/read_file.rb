# encoding: utf-8

module FileWatch module ReadHandlers
  class ReadFile < Base
    def handle_specifically(watched_file)
      if open_file(watched_file)
        add_or_update_sincedb_collection(watched_file) unless sincedb_collection.member?(watched_file.sincedb_key)
        # if the `read_iterations` * `file_chunk_size` is less than the file size
        # then this method will be executed multiple times
        # and the seek is moved to just after a line boundary as recorded in the sincedb
        # for each run - so we reset the buffer
        watched_file.reset_buffer
        watched_file.file_seek(watched_file.bytes_read)
        changed = false
        OPTS.read_iterations.times do
          begin
            lines = watched_file.buffer_extract(watched_file.file_read(OPTS.file_chunk_size))
            logger.warn("read_to_eof: no delimiter found in current chunk") if lines.empty?
            changed = true
            lines.each do |line|
              watched_file.listener.accept(line)
              sincedb_collection.increment(watched_file.sincedb_key, line.bytesize + OPTS.delimiter_byte_size)
            end
          rescue EOFError
            # flush the buffer now in case there is no final delimiter
            line = watched_file.buffer.flush
            watched_file.listener.accept(line) unless line.empty?
            watched_file.listener.eof
            watched_file.file_close
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
  end
end end
