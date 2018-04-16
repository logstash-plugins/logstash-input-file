# encoding: utf-8

module FileWatch
  # Tracks the position and expiry of the offset of a file-of-interest
  class SincedbValue
    attr_reader :last_changed_at, :watched_file, :path_in_sincedb

    def initialize(position, last_changed_at = nil, watched_file = nil)
      @position = position # this is the value read from disk
      @last_changed_at = last_changed_at
      @watched_file = watched_file
      touch if @last_changed_at.nil? || @last_changed_at.zero?
    end

    def add_path_in_sincedb(path)
      @path_in_sincedb = path # can be nil
      self
    end

    def last_changed_at_expires(duration)
      @last_changed_at + duration
    end

    def position
      # either the value from disk or the current wf position
      if @watched_file.nil?
        @position
      else
        @watched_file.bytes_read
      end
    end

    def update_position(pos)
      touch
      if @watched_file.nil?
        @position = pos
      else
        @watched_file.update_bytes_read(pos)
      end
    end

    def increment_position(pos)
      touch
      if watched_file.nil?
        @position += pos
      else
        @watched_file.increment_bytes_read(pos)
      end
    end

    def set_watched_file(watched_file)
      touch
      @watched_file = watched_file
    end

    def touch
      @last_changed_at = Time.now.to_f
    end

    def to_s
      # consider serializing the watched_file state as well
      "#{position} #{last_changed_at}".tap do |s|
        if @watched_file.nil?
          s.concat(" ").concat(@path_in_sincedb) unless @path_in_sincedb.nil?
        else
          s.concat(" ").concat(@watched_file.path)
        end
      end
    end

    def clear_watched_file
      @watched_file = nil
    end

    def unset_watched_file
      # cache the position
      # we don't cache the path here because we know we are done with this file.
      # either due via the `delete` handling
      # or when read mode is done with a file.
      # in the case of `delete` if the file was renamed then @watched_file is the
      # watched_file of the previous path and the new path will be discovered and
      # it should have the same inode as before.
      # The key from the new watched_file should then locate this entry and we
      # can resume from the cached position
      return if @watched_file.nil?
      wf = @watched_file
      @watched_file = nil
      @position = wf.bytes_read
    end
  end
end
