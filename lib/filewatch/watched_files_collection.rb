# encoding: utf-8

require 'java'

module FileWatch
  class WatchedFilesCollection

    # @return truthy if file was added as a new mapping, falsy otherwise
    def add(watched_file)
      path = watched_file.path.freeze
      synchronize do
        prev_path = _put_file(watched_file, path)
        @files_inverse.delete(prev_path) if prev_path
        @files_inverse[path] = watched_file
        prev_path
      end
    end

    def remove_paths(paths)
      paths = Array(paths)
      return if paths.empty?
      removed_files = []
      synchronize do
        Array(paths).each do |path|
          file = @files_inverse.delete(path)
          if file
            _remove_file(file)
            removed_files << file
          end
        end
      end
      removed_files
    end

    def close_all
      synchronize { @files.each_key(&:file_close) }
    end

    def empty?
      synchronize { @files.empty? }
    end

    # @return [Enumerable<String>] managed path keys (snapshot)
    alias keys paths

    # @return [Enumerable<WatchedFile>] managed files (snapshot)
    alias values files

    # @param path [String] the file path
    # @return [File] or nil if not found
    def get(path)
      synchronize { @files_inverse[path] }
    end

  end
end
