# encoding: utf-8

require 'java'

module FileWatch
  # @see `org.logstash.filewatch.WatchedFilesCollection`
  class WatchedFilesCollection

    # Closes all managed watched files.
    # @see FileWatch::WatchedFile#file_close
    def close_all
      each_file(&:file_close) # synchronized
    end

    # @return [Enumerable<String>] managed path keys (snapshot)
    alias keys paths

    # @return [Enumerable<WatchedFile>] managed files (snapshot)
    alias values files

  end
end
