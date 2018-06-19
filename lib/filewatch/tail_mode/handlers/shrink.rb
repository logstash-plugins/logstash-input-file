# encoding: utf-8

module FileWatch module TailMode module Handlers
  class Shrink < Base
    def handle_specifically(watched_file)
      add_or_update_sincedb_collection(watched_file)
      watched_file.file_seek(watched_file.bytes_read)
      logger.trace("reading to eof: #{watched_file.path}")
      read_to_eof(watched_file)
    end

    def update_existing_specifically(watched_file, sincedb_value)
      # we have a match but size is smaller
      # set all to zero
      logger.trace("update_existing_specifically: #{watched_file.path}: was truncated seeking to beginning")
      watched_file.reset_bytes_unread
      sincedb_value.update_position(0)
    end
  end
end end end
