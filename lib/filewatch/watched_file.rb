# encoding: utf-8
require_relative 'bootstrap'

module FileWatch
  class WatchedFile
    def self.win_inode(path, stat)
      fileId = Winhelper.GetWindowsUniqueFileIdentifier(path)
      [fileId, 0, 0] # dev_* doesn't make sense on Windows
    end

    def self.nix_inode(path, stat)
      [stat.ino.to_s, stat.dev_major, stat.dev_minor]
    end

    def self.inode(path, stat)
      # removed the send(sym, ...) call as
      # its slow and this is called on each restat
      if FILEWATCH_INODE_METHOD == :win_inode
        win_inode(path, stat)
      else
        nix_inode(path, stat)
      end
    end

    def self.new_initial(path, stat)
      new(path, stat, true).from_discover
    end

    attr_reader :bytes_read, :state, :file, :buffer, :state_history
    attr_reader :path, :filestat, :accessed_at, :modified_at, :pathname
    attr_reader :sdb_key_v1, :last_stat_size, :listener
    attr_accessor :last_open_warning_at

    # this class represents a file that has been discovered
    def initialize(pathname, stat, initial)
      @pathname = Pathname.new(pathname) # given arg pathname might be a string or a Pathname object
      @path = @pathname.to_path
      @bytes_read = 0
      @last_stat_size = 0
      @sdb_key_v1 = InodeStruct.new(*self.class.inode(path, stat))
      # initial as true means we have not associated this watched_file with a previous sincedb value yet.
      # and we should read from the beginning if necessary
      @initial = initial
      @state_history = []
      @state = :watched
      set_stat(stat) # can change @last_stat_size
      @listener = NullListener.new(@path)
      @last_open_warning_at = nil
      set_accessed_at
    end

    def set_listener(observer)
      @listener = observer.listener_for(@path)
    end

    def unset_listener
      @listener = nil
    end

    def has_listener?
      !@listener.is_a?(NullListener)
    end

    def sincedb_key
      @sdb_key_v1
    end

    def from_discover
      @discovered = true
      self
    end

    def initial_completed
      @initial = false
    end

    def set_accessed_at
      @accessed_at = Time.now.to_f
    end

    def discovered?
      @discovered
    end

    def initial?
      @initial
    end

    def compressed?
      @path.end_with?('.gz','.gzip')
    end

    def size_changed?
      @last_stat_size != bytes_read
    end

    def all_read?
      @last_stat_size == bytes_read
    end

    def open
      file_add_opened(FileOpener.open(@path))
    end

    def file_add_opened(rubyfile)
      @file = rubyfile
      @buffer = BufferedTokenizer.new(OPTS.delimiter) if @buffer.nil?
    end

    def file_close
      return if @file.nil? || @file.closed?
      @file.close
      @file = nil
    end

    def file_seek(amount, whence = IO::SEEK_SET)
      @file.sysseek(amount, whence)
    end

    def file_read(amount)
      set_accessed_at
      @file.sysread(amount)
    end

    def file_open?
      !@file.nil? && !@file.closed?
    end

    def reset_buffer
      @buffer.input.clear
    end

    def buffer_extract(data)
      @buffer.extract(data)
    end

    def buffer_delimiter_byte_size
      @buffer.delimiter_byte_size
    end

    def increment_bytes_read(delta)
      return if delta.nil?
      @bytes_read += delta
    end

    def update_bytes_read(total_bytes_read)
      return if total_bytes_read.nil?
      @bytes_read = total_bytes_read
    end

    def update_path(_path)
      @path = _path
    end

    def update_stat(st)
      set_stat(st)
    end

    def activate
      set_state :active
    end

    def ignore
      set_state :ignored
      @bytes_read = @filestat.size
    end

    def close
      set_state :closed
    end

    def watch
      set_state :watched
    end

    def unwatch
      set_state :unwatched
    end

    def active?
      @state == :active
    end

    def ignored?
      @state == :ignored
    end

    def closed?
      @state == :closed
    end

    def watched?
      @state == :watched
    end

    def unwatched?
      @state == :unwatched
    end

    def expiry_close_enabled?
      !OPTS.close_older.nil?
    end

    def expiry_ignore_enabled?
      !OPTS.ignore_older.nil?
    end

    def shrunk?
      @last_stat_size < @bytes_read
    end

    def grown?
      @last_stat_size > @bytes_read
    end

    def restat
      set_stat(pathname.stat)
    end

    def set_state(value)
      @state_history << @state
      @state = value
    end

    def state_history_any?(*previous)
      (@state_history & previous).any?
    end

    def full_state_history
      @state_history + Array(@state)
    end

    def file_closable?
      file_can_close? && all_read?
    end

    def file_ignorable?
      return false unless expiry_ignore_enabled?
      # (Time.now - stat.mtime) <- in jruby, this does int and float
      # conversions before the subtraction and returns a float.
      # so use all floats upfront
      (Time.now.to_f - @modified_at) > OPTS.ignore_older
    end

    def file_can_close?
      return false unless expiry_close_enabled?
      (Time.now.to_f - @accessed_at) > OPTS.close_older
    end

    def to_s
      inspect
    end

    private

    def set_stat(stat)
      @modified_at = stat.mtime.to_f
      @last_stat_size = stat.size
      @filestat = stat
    end
  end
end
