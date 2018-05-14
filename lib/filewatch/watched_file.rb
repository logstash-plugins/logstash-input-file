# encoding: utf-8

module FileWatch
  class WatchedFile
    include InodeMixin # see bootstrap.rb at `if LogStash::Environment.windows?`

    attr_reader :bytes_read, :state, :file, :buffer, :recent_states, :bytes_unread
    attr_reader :path, :filestat, :accessed_at, :modified_at, :pathname
    attr_reader :sdb_key_v1, :last_stat_size, :listener, :read_loop_count, :read_chunk_size
    attr_accessor :last_open_warning_at

    # this class represents a file that has been discovered
    # path based stat is taken at discovery
    def initialize(pathname, stat, settings)
      @settings = settings
      @pathname = Pathname.new(pathname) # given arg pathname might be a string or a Pathname object
      @path = @pathname.to_path
      @bytes_read = 0 # tracks bytes read from the open file or initialized from a matched sincedb_value off disk.
      @bytes_unread = 0 # tracks bytes not yet read from the open file. So we can warn on shrink when unread bytes are seen.
      @last_stat_size = 0
      @previous_stat_size = 0
      # the prepare_inode method is sourced from the mixed module above
      @sdb_key_v1 = InodeStruct.new(*prepare_inode(path, stat))
      # initial as true means we have not associated this watched_file with a previous sincedb value yet.
      # and we should read from the beginning if necessary
      @initial = true
      @recent_states = [] # keep last 8 states, managed in set_state
      @state = :watched
      set_stat(stat) # can change @last_stat_size
      set_previous_stat_size
      @listener = nil
      @last_open_warning_at = nil
      @pending_inode_count = 0
      @read_loop_count = @settings.file_chunk_count
      @read_chunk_size = @settings.file_chunk_size
      set_accessed_at
    end

    def set_listener(observer)
      @listener = observer.listener_for(@path)
    end

    def unset_listener
      @listener = nil
    end

    def has_listener?
      !@listener.nil?
    end

    def sincedb_key
      @sdb_key_v1
    end

    def initial_completed
      @initial = false
    end

    def set_accessed_at
      @accessed_at = Time.now.to_f
    end

    def initial?
      @initial
    end

    def compressed?
      @path.end_with?('.gz','.gzip')
    end

    def size_changed?
      (@last_stat_size != @previous_stat_size)
    end

    def all_read?
      @last_stat_size == bytes_read && @last_stat_size > 0
    end

    def open
      file_add_opened(FileOpener.open(@path))
    end

    def file_add_opened(rubyfile)
      @file = rubyfile
      @buffer = BufferedTokenizer.new(@settings.delimiter) if @buffer.nil?
    end

    def file_close
      return if @file.nil? || @file.closed?
      @file.close
      @file = nil
    end

    def file_seek(amount, whence = IO::SEEK_SET)
      @file.sysseek(amount, whence)
    end

    def file_read(amount = nil)
      set_accessed_at
      @file.sysread(amount || @read_chunk_size)
    end

    def file_open?
      !@file.nil? && !@file.closed?
    end

    def reset_buffer
      @buffer.flush
    end

    def read_extract_lines
      data = file_read
      result = buffer_extract(data)
      increment_bytes_read(data.bytesize)
      result
    end

    def buffer_extract(data)
      warning, additional = "", {}
      lines = @buffer.extract(data)
      if lines.empty?
        warning.concat("buffer_extract: a delimiter can't be found in current chunk")
        warning.concat(", maybe there are no more delimiters or the delimiter is incorrect")
        warning.concat(" or the text before the delimiter, a 'line', is very large")
        warning.concat(", if this message is logged often try increasing the `file_chunk_size` setting.")
        additional["delimiter"] = @settings.delimiter
        additional["read_position"] = @bytes_read
        additional["bytes_read_count"] = data.bytesize
        additional["last_known_file_size"] = @last_stat_size
        additional["file_path"] = @path
      end
      BufferExtractResult.new(lines, warning, additional)
    end

    def increment_bytes_read(delta)
      return if delta.nil?
      @bytes_read += delta
      update_bytes_unread
      @bytes_read
    end

    def update_bytes_read(total_bytes_read)
      return if total_bytes_read.nil?
      @bytes_read = total_bytes_read
      update_bytes_unread
      @bytes_read
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
      set_previous_stat_size
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
      !@settings.close_older.nil?
    end

    def expiry_ignore_enabled?
      !@settings.ignore_older.nil?
    end

    def shrunk?
      @last_stat_size < @bytes_read
    end

    def grown?
      @last_stat_size > @bytes_read
    end

    def new_inode_detected?
      @pending_inode_count > 0
    end

    def restat
      path_based_stat = pathname.stat
      if file_open?
        set_stat(@file.to_io.stat)
      else
        set_stat(path_based_stat)
      end
      # check for a change in inode aka rotated file
      if path_based_stat.ino.to_s != sincedb_key.inode
        @pending_inode_count = @pending_inode_count.succ
      end
    end

    def set_sincedb_key_from_stat(stat = pathname.stat)
      set_stat(stat)
      @sdb_key_v1 = InodeStruct.new(*prepare_inode(@path, stat))
      @pending_inode_count = 0
    end

    def set_depth_first_read_loop
      @read_loop_count = FileWatch::FIXNUM_MAX
      @read_chunk_size = FileWatch::FILE_READ_SIZE
    end

    def set_user_defined_read_loop
      @read_loop_count = @settings.file_chunk_count
      @read_chunk_size = @settings.file_chunk_size
    end

    def reset_bytes_unread
      # call from shrink
      @bytes_unread = 0
    end

    def set_state(value)
      @recent_states.shift if @recent_states.size == 8
      @recent_states << @state
      @state = value
    end

    def recent_state_history
      @recent_states + Array(@state)
    end

    def file_closable?
      file_can_close? && all_read?
    end

    def file_ignorable?
      return false unless expiry_ignore_enabled?
      # (Time.now - stat.mtime) <- in jruby, this does int and float
      # conversions before the subtraction and returns a float.
      # so use all floats upfront
      (Time.now.to_f - @modified_at) > @settings.ignore_older
    end

    def file_can_close?
      return false unless expiry_close_enabled?
      (Time.now.to_f - @accessed_at) > @settings.close_older
    end

    def to_s
      inspect
    end

    private

    def set_stat(stat)
      return if stat_unchanged?(stat)
      @modified_at = stat.mtime.to_f
      set_previous_stat_size
      @last_stat_size = stat.size
      update_bytes_unread
      @filestat = stat
    end

    def set_previous_stat_size
      @previous_stat_size = @last_stat_size
    end

    def update_bytes_unread
      unread = (@previous_stat_size.zero? ? @last_stat_size : @previous_stat_size) - @bytes_read
      @bytes_unread = unread
      @bytes_unread = 0 if unread < 0
    end

    def stat_unchanged?(other)
      return false if @filestat.nil?
      @filestat.dev == other.dev && @filestat.ino == other.ino && @filestat.size == other.size && @filestat.mtime == other.mtime
    end
  end
end
