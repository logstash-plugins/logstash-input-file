# encoding: utf-8

module LogStash module Inputs
  class DeleteCompletedFileHandler
    def initialize(watch)
      @watch = watch
    end

    def handle(path)
      Pathname.new(path).unlink rescue nil
      @watch.watched_files_collection.remove_paths([path])
    end
  end
end end
