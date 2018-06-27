# encoding: utf-8

module FileWatch
  class WatchedFile
    PATH_BASED_STAT = 0
    IO_BASED_STAT = 1

    attr_reader :bytes_read, :state, :file, :buffer, :recent_states, :bytes_unread
    attr_reader :path, :accessed_at, :modified_at, :pathname, :filename
    attr_reader :listener, :read_loop_count, :read_chunk_size, :stats, :read_bytesize_description
    attr_accessor :last_open_warning_at

    # this class represents a file that has been discovered
    # path based stat is taken at discovery
    def initialize(pathname, stat, settings)
      @settings = settings
      @pathname = Pathname.new(pathname) # given arg pathname might be a string or a Pathname object
      @path = @pathname.to_path
      @filename = @pathname.basename.to_s
      @stats = []
      @stat_index = PATH_BASED_STAT
      full_state_reset(stat)
      watch
      set_user_defined_read_loop
      set_accessed_at
    end

    def filestat
      @stats[@stat_index]
    end

    def no_restat_reset
      full_state_reset(@stats[PATH_BASED_STAT])
    end

    def full_state_reset(this_stat = nil)
      if this_stat.nil?
        begin
          this_stat = PathStatClass.new(pathname)
        rescue Errno::ENOENT
          delay_delete
          return
        end
      end
      @bytes_read = 0 # tracks bytes read from the open file or initialized from a matched sincedb_value off disk.
      @bytes_unread = 0 # tracks bytes not yet read from the open file. So we can warn on shrink when unread bytes are seen.
      file_close
      @stats[PATH_BASED_STAT] = this_stat
      @listener = nil
      @last_open_warning_at = nil
      # initial as true means we have not associated this watched_file with a previous sincedb value yet.
      # and we should read from the beginning if necessary
      @initial = true
      @recent_states = [] # keep last 8 states, managed in set_state
      # the prepare_inode method is sourced from the mixed module above
      @sdb_key_v1 = filestat.inode_struct
      watch if active? || @state.nil?
    end

    def rotate_from(other)
      # move all state from other to this one
      set_user_defined_read_loop
      file_close
      @bytes_read = other.bytes_read
      @bytes_unread = other.bytes_unread
      @listener = nil
      @initial = false
      @recent_states = other.recent_states
      @accessed_at = other.accessed_at
      if !other.delayed_delete?
        # we don't know if a file exists at the other.path yet
        # so no reset
        other.full_state_reset
      end
      @stat_index = PATH_BASED_STAT
      @stats[PATH_BASED_STAT] = PathStatClass.new(pathname)
      @sdb_key_v1 = filestat.inode_struct
      ignore
    end

    def rotate_as_initial_file
      # rotation, when no sincedb record exists for new inode - we have never seen this inode before.
      rotate_as_file
      @initial = true
    end

    def rotate_as_file(bytes_read = 0)
      # rotation, when a sincedb record exists for new inode, but no watched file to rotate from
      # probably caused by a deletion detected in the middle of the rename cascade
      # RARE due to delayed_delete - there would have to be a large time span between the renames.
      @bytes_read = bytes_read # tracks bytes read from the open file or initialized from a matched sincedb_value off disk.
      @bytes_unread = 0 # tracks bytes not yet read from the open file. So we can warn on shrink when unread bytes are seen.
      @last_open_warning_at = nil
      # initial as true means we have not associated this watched_file with a previous sincedb value yet.
      # and we should read from the beginning if necessary
      @initial = false
      @recent_states = [] # keep last 8 states, managed in set_state
      @stat_index = PATH_BASED_STAT
      @stats[@stat_index] = PathStatClass.new(pathname)
      reopen
      @sdb_key_v1 = filestat.inode_struct
      watch
    end

    def file_at_path_found_again
      restore_previous_state
    end

    def path_based_sincedb_key
      @stats[PATH_BASED_STAT].inode_struct
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
      # called from closed and ignored
      # before the last stat was taken file should be fully read.
      last_stat_size != @bytes_read
    end

    def all_read?
      test_bytes_read(last_stat_size)
    end

    def all_open_file_bytes_read?
      test_bytes_read(@stats[IO_BASED_STAT].size)
    end

    def reopen
      if file_open?
        file_close
        open
      end
    end

    def open
      file_add_opened(FileOpener.open(@path))
    end

    def file_add_opened(rubyfile)
      @file = rubyfile
      @stat_index = IO_BASED_STAT
      @stats[IO_BASED_STAT] = IOStatClass.new(@file.to_io).add_identifier(@stats[PATH_BASED_STAT].identifier)
      @buffer = BufferedTokenizer.new(@settings.delimiter) if @buffer.nil?
    end

    def file_close
      @stat_index = PATH_BASED_STAT
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
        additional["last_known_file_size"] = last_stat_size
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

    def rotation_in_progress
      set_state :rotation_in_progress
    end

    def activate
      set_state :active
    end

    def ignore
      set_state :ignored
    end

    def ignore_as_unread
      ignore
      @bytes_read = filestat.size
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

    def delay_delete
      set_state :delayed_delete
    end

    def restore_previous_state
      set_state @recent_states.pop
    end

    def rotation_in_progress?
      @state == :rotation_in_progress
    end

    def active?
      @state == :active
    end

    def delayed_delete?
      @state == :delayed_delete
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
      active_stat_size < @bytes_read
    end

    def grown?
      active_stat_size > @bytes_read
    end

    def rotation_detected?
      if file_open?
        return false if @stats[IO_BASED_STAT].nil?
        @stats[PATH_BASED_STAT].inode != @stats[IO_BASED_STAT].inode
      else
        path_based_sincedb_key != sincedb_key
      end
    end

    def restat
      @stats[PATH_BASED_STAT].restat
      if file_open?
        @stats[IO_BASED_STAT].restat
      end
      update_bytes_unread
    end

    def set_depth_first_read_loop
      @read_loop_count = FileWatch::MAX_ITERATIONS
      @read_chunk_size = FileWatch::FILE_READ_SIZE
      @read_bytesize_description = "All"
    end

    def set_user_defined_read_loop
      @read_loop_count = @settings.file_chunk_count
      @read_chunk_size = @settings.file_chunk_size
      @read_bytesize_description = @read_loop_count == FileWatch::MAX_ITERATIONS ? "All" : (@read_loop_count * @read_chunk_size).to_s
    end

    def reset_bytes_unread
      # called from shrink
      @bytes_unread = 0
    end

    def set_state(value)
      @recent_states.shift if @recent_states.size == 8
      @recent_states << @state unless @state.nil?
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
      (Time.now.to_f - modified_at) > @settings.ignore_older
    end

    def file_can_close?
      return false unless expiry_close_enabled?
      (Time.now.to_f - @accessed_at) > @settings.close_older
    end

    def details
      detail = "@filename='#{filename}', @state='#{state}', @recent_states='#{@recent_states.inspect}', "
      detail.concat("@bytes_read='#{@bytes_read}', @bytes_unread='#{@bytes_unread}', last_stat_size='#{last_stat_size}', ")
      detail.concat("file_open?=='#{file_open?}'")
      "<FileWatch::WatchedFile: #{detail}, @sincedb_key='#{sincedb_key}'>"
    end

    def inspect
      "\"<FileWatch::WatchedFile: @filename='#{filename}', @state='#{state}', @sincedb_key='#{sincedb_key}'>\""
    end

    def to_s
      inspect
    end

    def active_stat_size
      rotation_detected? ? @stats[PATH_BASED_STAT].size : last_stat_size
    end

    def modified_at
      filestat.modified_at
    end

    def last_stat_size
      filestat.size
    end

    private

    def test_bytes_read(size)
      bytes_read >= size
    end

    def update_bytes_unread
      unread = active_stat_size - @bytes_read
      @bytes_unread = unread
      @bytes_unread = 0 if unread < 0
    end
  end
end
