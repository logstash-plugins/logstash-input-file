# encoding: utf-8

module FileWatch module TailMode module Handlers
  class Shrink < Base
    def handle_specifically(watched_file)
      sdbv = add_or_update_sincedb_collection(watched_file)
      watched_file.file_seek(watched_file.bytes_read)
      logger.trace("reading to eof...", "file name" => watched_file.filename)
      read_to_eof(watched_file)
      logger.trace("handle_specifically: after read_to_eof", "watched file" => watched_file.details, "sincedb value" => sdbv)
    end

    def update_existing_specifically(watched_file, sincedb_value)
      # we have a match but size is smaller
      # set all to zero
      watched_file.reset_bytes_unread
      sincedb_value.update_position(0)
      logger.trace("update_existing_specifically: was truncated seeking to beginning", "watched file" => watched_file.details, "sincedb value" => sincedb_value)
    end
  end
end end end
