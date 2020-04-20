# encoding: utf-8

require 'java'
require 'thread'

module FileWatch
  class WatchedFilesCollection

    def initialize(settings)
      @sort_by = settings.file_sort_by.to_sym # "last_modified" | "path"
      @sort_direction = settings.file_sort_direction.to_sym # "asc" | "desc"

      case @sort_by
      when :last_modified
        sorter = @sort_direction.eql?(:desc) ?
                     -> (l, r) { r.modified_at <=> l.modified_at } :
                     -> (l, r) { l.modified_at <=> r.modified_at }
      when :path
        sorter = @sort_direction.eql?(:desc) ?
                     -> (l, r) { r.path <=> l.path } :
                     -> (l, r) { l.path <=> r.path }
      else
        raise ArgumentError, "sort_by: #{@sort_by.inspect} not supported"
      end

      @files = java.util.TreeMap.new(&sorter) # [WatchedFile] -> [String] file.path
      @files_lock = Thread::Mutex.new
      @files_inverse = Hash.new # keep an inverse view for fast path lookups
    end

    # @return truthy if file was added as a new mapping, falsy otherwise
    def add(watched_file)
      path = watched_file.path.freeze
      @files_lock.synchronize do
        prev_path = @files.put(watched_file, path)
        @files_inverse.delete(prev_path) if prev_path
        @files_inverse[path] = watched_file
        prev_path
      end
    end

    def remove_paths(paths)
      removed_files = []
      @files_lock.synchronize do
        Array(paths).each do |path|
          file = @files_inverse.delete(path)
          if file
            @files.remove(file)
            removed_files << file
          end
        end
      end
      removed_files
    end

    def close_all
      @files_lock.synchronize { @files.each_key(&:file_close) }
    end

    def empty?
      @files_lock.synchronize { @files.empty? }
    end

    # @return [Enumerable<String>] managed path keys (snapshot)
    def keys
      @files_lock.synchronize { @files.values.to_a }
    end

    # @return [Enumerable<File>] managed files (snapshot)
    def values
      # NOTE: needs to return properly ordered files
      @files_lock.synchronize { @files.key_set.to_a }
    end

    # @param path [String] the file path
    # @return [File] or nil if not found
    def get(path)
      @files_lock.synchronize { @files_inverse[path] }
    end

  end
end
