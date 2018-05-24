# encoding: utf-8

module FileWatch module TailMode module Handlers
  class Delete < Base
    DATA_LOSS_WARNING = "file deleted or renamed with unread bytes, however if the file is found it will be read from the last position"
    def handle_specifically(watched_file)
      # TODO consider trying to find the renamed file - it will have the same inode.
      # Needs a rotate scheme rename hint from user e.g. "<name>-YYYY-MM-DD-N.<ext>" or "<name>.<ext>.N"
      # send the found content to the same listener (stream identity)
      logger.debug("info",
        "sincedb_key" => watched_file.sincedb_key,
        "size" => watched_file.last_stat_size,
        "previous inode size" => watched_file.previous_inode_size,
        "active size" => watched_file.active_stat_size,
        "pending_inode_count" => watched_file.pending_inode_count,
        "read" => watched_file.bytes_read,
        "unread" => watched_file.bytes_unread,
        "path" => watched_file.path)
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
