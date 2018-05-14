# encoding: utf-8

module FileWatch module TailMode module Handlers
  class Delete < Base
    DATA_LOSS_WARNING = "file deleted or renamed with unread bytes, however if the file is found it will be read from the last position"
    def handle_specifically(watched_file)
      if watched_file.bytes_unread > 0
        logger.warn(DATA_LOSS_WARNING, "unread_bytes" => watched_file.bytes_unread, "path" => watched_file.path)
      end
      watched_file.listener.deleted
      # no need to worry about data in the buffer
      # if found it will be associated by inode and read from last position
      sincedb_collection.unset_watched_file(watched_file)
      watched_file.file_close
    end
  end
end end end
