# encoding: utf-8

module FileWatch module TailMode module Handlers
  class Grow < Base
    def handle_specifically(watched_file)
      watched_file.file_seek(watched_file.bytes_read)
      read_to_eof(watched_file)
    end
  end
end end end
