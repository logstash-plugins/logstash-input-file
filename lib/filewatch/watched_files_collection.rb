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

      @files = java.util.TreeMap.new(&sorter) # [File] -> [String] file.path
      @files_lock = Thread::Mutex.new
    end

    def add(watched_file)
      @files_lock.synchronize { !@files.put(watched_file, watched_file.path).nil? }
    end

    def remove_paths(paths)
      removed_files = []
      @files_lock.synchronize do
        Array(paths).each do |path|
          entry = file_for_path(path)
          if entry
            watched_file = entry.key
            @files.remove(watched_file)
            removed_files << watched_file
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
      @files_lock.synchronize { @files.key_set.to_a }
    end

    # @param path [String] the file path
    # @return [File] or nil if not found
    def get(path)
      @files_lock.synchronize { file_for_path(path) }
    end

    private

    def file_for_path(path)
      entry = @files.entry_set.find { |_, val| val == path }
      entry && entry.key
    end

  end
end
