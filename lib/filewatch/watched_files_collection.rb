# encoding: utf-8
require_relative 'bootstrap' unless defined?(FileWatch)

module FileWatch
  class WatchedFilesCollection

    def initialize
      @files = {}
    end

    def add(watched_file)
      @files[watched_file.path] = watched_file
    end

    def delete(paths)
      Array(paths).each {|f| @files.delete(f)}
    end

    def close_all
      @files.values.each(&:file_close)
    end

    def empty?
      @files.empty?
    end

    def keys
      @files.keys
    end

    def values
      @files.values
    end

    def watched_file_by_path(path)
      @files[path]
    end
  end
end
