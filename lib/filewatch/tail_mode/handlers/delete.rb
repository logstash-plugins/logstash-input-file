# encoding: utf-8

module FileWatch module TailMode module Handlers
  class Delete < Base
    def handle_specifically(watched_file)
      watched_file.listener.deleted
      sincedb_collection.unset_watched_file(watched_file)
      watched_file.file_close
    end
  end
end end end
