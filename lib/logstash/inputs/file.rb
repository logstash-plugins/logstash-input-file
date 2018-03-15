# encoding: utf-8
require "logstash/namespace"
require "logstash/inputs/base"
require "logstash/codecs/identity_map_codec"

require "pathname"
require "socket" # for Socket.gethostname
require "fileutils"

require_relative "file/patch"
require_relative "file_listener"
require_relative "delete_completed_file_handler"
require_relative "log_completed_file_handler"

# Stream events from files, normally by tailing them in a manner
# similar to `tail -0F` but optionally reading them from the
# beginning.
#
# By default, each event is assumed to be one line and a line is
# taken to be the text before a newline character.
# Normally, logging will add a newline to the end of each line written.

# If you would like to join multiple log lines into one event,
# you'll want to use the multiline codec or filter.
#
# The plugin aims to track changing files and emit new content as it's
# appended to each file. It's not well-suited for reading a file from
# beginning to end and storing all of it in a single event (not even
# with the multiline codec or filter).
#
# ==== Reading from remote network volumes
#
# The file input is not tested on remote filesystems such as NFS, Samba, s3fs-fuse, etc. These 
# remote filesystems typically have behaviors that are very different from local filesystems and 
# are therefore unlikely to work correctly when used with the file input.
#
# ==== Tracking of current position in watched files
#
# The plugin keeps track of the current position in each file by
# recording it in a separate file named sincedb. This makes it
# possible to stop and restart Logstash and have it pick up where it
# left off without missing the lines that were added to the file while
# Logstash was stopped.
#
# By default, the sincedb file is placed in the home directory of the
# user running Logstash with a filename based on the filename patterns
# being watched (i.e. the `path` option). Thus, changing the filename
# patterns will result in a new sincedb file being used and any
# existing current position state will be lost. If you change your
# patterns with any frequency it might make sense to explicitly choose
# a sincedb path with the `sincedb_path` option.
#
# A different `sincedb_path` must be used for each input. Using the same
# path will cause issues. The read checkpoints for each input must be
# stored in a different path so the information does not override.
#
# Sincedb files are text files with four columns:
#
# . The inode number (or equivalent).
# . The major device number of the file system (or equivalent).
# . The minor device number of the file system (or equivalent).
# . The current byte offset within the file.
#
# On non-Windows systems you can obtain the inode number of a file
# with e.g. `ls -li`.
#
# ==== File rotation
#
# File rotation is detected and handled by this input, regardless of
# whether the file is rotated via a rename or a copy operation. To
# support programs that write to the rotated file for some time after
# the rotation has taken place, include both the original filename and
# the rotated filename (e.g. /var/log/syslog and /var/log/syslog.1) in
# the filename patterns to watch (the `path` option). Note that the
# rotated filename will be treated as a new file so if
# `start_position` is set to 'beginning' the rotated file will be
# reprocessed.
#
# With the default value of `start_position` ('end') any messages
# written to the end of the file between the last read operation prior
# to the rotation and its reopening under the new name (an interval
# determined by the `stat_interval` and `discover_interval` options)
# will not get picked up.
module LogStash module Inputs
class File < LogStash::Inputs::Base
  config_name "file"

  # The path(s) to the file(s) to use as an input.
  # You can use filename patterns here, such as `/var/log/*.log`.
  # If you use a pattern like `/var/log/**/*.log`, a recursive search
  # of `/var/log` will be done for all `*.log` files.
  # Paths must be absolute and cannot be relative.
  #
  # You may also configure multiple paths. See an example
  # on the <<array,Logstash configuration page>>.
  config :path, :validate => :array, :required => true

  # Exclusions (matched against the filename, not full path). Filename
  # patterns are valid here, too. For example, if you have
  # [source,ruby]
  #     path => "/var/log/*"
  #
  # You might want to exclude gzipped files:
  # [source,ruby]
  #     exclude => "*.gz"
  config :exclude, :validate => :array

  # How often (in seconds) we stat files to see if they have been modified.
  # Increasing this interval will decrease the number of system calls we make,
  # but increase the time to detect new log lines.
  config :stat_interval, :validate => :number, :default => 1

  # How often (in seconds) we expand the filename patterns in the
  # `path` option to discover new files to watch.
  config :discover_interval, :validate => :number, :default => 15

  # Path of the sincedb database file (keeps track of the current
  # position of monitored log files) that will be written to disk.
  # The default will write sincedb files to `<path.data>/plugins/inputs/file`
  # NOTE: it must be a file path and not a directory path
  config :sincedb_path, :validate => :string

  # How often (in seconds) to write a since database with the current position of
  # monitored log files.
  config :sincedb_write_interval, :validate => :number, :default => 15

  # Choose where Logstash starts initially reading files: at the beginning or
  # at the end. The default behavior treats files like live streams and thus
  # starts at the end. If you have old data you want to import, set this
  # to 'beginning'.
  #
  # This option only modifies "first contact" situations where a file
  # is new and not seen before, i.e. files that don't have a current
  # position recorded in a sincedb file read by Logstash. If a file
  # has already been seen before, this option has no effect and the
  # position recorded in the sincedb file will be used.
  config :start_position, :validate => [ "beginning", "end"], :default => "end"

  # set the new line delimiter, defaults to "\n"
  config :delimiter, :validate => :string, :default => "\n"

  # When the file input discovers a file that was last modified
  # before the specified timespan in seconds, the file is ignored.
  # After it's discovery, if an ignored file is modified it is no
  # longer ignored and any new data is read. By default, this option is
  # disabled. Note this unit is in seconds.
  config :ignore_older, :validate => :number

  # The file input closes any files that were last read the specified
  # timespan in seconds ago.
  # This has different implications depending on if a file is being tailed or
  # read. If tailing, and there is a large time gap in incoming data the file
  # can be closed (allowing other files to be opened) but will be queued for
  # reopening when new data is detected. If reading, the file will be closed
  # after closed_older seconds from when the last bytes were read.
  # The default is 1 hour
  config :close_older, :validate => :number, :default => 1 * 60 * 60

  # What is the maximum number of file_handles that this input consumes
  # at any one time. Use close_older to close some files if you need to
  # process more files than this number. This should not be set to the
  # maximum the OS can do because file handles are needed for other
  # LS plugins and OS processes.
  # The default of 4095 is set in filewatch.
  config :max_open_files, :validate => :number

  # What mode do you want the file input to operate in.
  # Tail a few files or read many content-complete files
  # The default is tail
  # If "read" is specified then the following other settings are ignored
  #   `start_position` (files are always read from the beginning)
  #   `delimiter` (files are assumed to use \n or \r (or both) as line endings)
  #   `close_older` (files are automatically 'closed' when EOF is reached)
  # If "read" is specified then the following settings are heeded
  #   `ignore_older` (older files are not processed)
  # "read" mode now supports gzip file processing
  config :mode, :validate => [ "tail", "read"], :default => "tail"

  # When in 'read' mode, what action should be carried out when a file is done with.
  # If 'delete' is specified then the file will be deleted.
  # If 'log' is specified then the full path of the file is logged to the file specified
  # in the `file_completed_log_path` setting.
  config :file_completed_action, :validate => ["delete", "log"], :default => "delete"

  # Which file should the completely read file paths be appended to.
  # Only specify this path to a file when `file_completed_action` is 'log'.
  config :file_completed_log_path, :validate => :string

  # The sincedb entry now has a last active timestamp associated with it.
  # If no changes are detected in tracked files in the last N days their sincedb
  # tracking record will expire and not be persisted.
  # This option protects against the well known inode recycling problem. (add reference)
  config :sincedb_clean_after, :validate => :number,  :default => 14 # days

  # This option sets up a form of striped read. Some lines from each active file will
  # appear be processed in parallel.
  # Given that the sort order of discovered files is OS dependent, it is not possible to control
  # the file processing order. Each read iteration will process a 32KB chunk,
  # e.g. 32 read iterations will process 1MB from each active file.
  # See the option `max_open_files` for more info, but note in "read" mode, close_older is
  # not required as files are closed at EOF
  # The default set internally is very large (the maximum positive Ruby integer)
  config :read_iterations, :validate => :number,  :default => (2**(0.size * 8 - 2) - 1)

  # Specify the size in bytes of each chunk read in each read iteration
  # See `read_iterations` to see why and when to change this from the default.
  # The default set internally is 32768 (32KB)
  config :file_chunk_size, :validate => :number,  :default => 32768

  public
  def register
    require "addressable/uri"
    require "digest/md5"
    @logger.trace("Registering file input", :path => @path)
    @host = Socket.gethostname.force_encoding(Encoding::UTF_8)
    # This check is Logstash 5 specific.  If the class does not exist, and it
    # won't in older versions of Logstash, then we need to set it to nil.
    settings = defined?(LogStash::SETTINGS) ? LogStash::SETTINGS : nil

    @filewatch_config = {
      :exclude => @exclude,
      :stat_interval => @stat_interval,
      :discover_interval => @discover_interval,
      :sincedb_write_interval => @sincedb_write_interval,
      :delimiter => @delimiter,
      :ignore_older => @ignore_older,
      :close_older => @close_older,
      :max_open_files => @max_open_files,
      :sincedb_clean_after => @sincedb_clean_after,
      :read_iterations => @read_iterations,
      :file_chunk_size => @file_chunk_size
    }

    @path.each do |path|
      if Pathname.new(path).relative?
        raise ArgumentError.new("File paths must be absolute, relative path specified: #{path}")
      end
    end

    if @sincedb_path.nil?
      if settings
        datapath = Pathname.new(settings.get_value("path.data")).join("plugins", "inputs", "file")
        # Ensure that the filepath exists before writing, since it's deeply nested.
        datapath.mkpath
        @sincedb_path = datapath.join(".sincedb_" + Digest::MD5.hexdigest(@path.join(",")))
      end
    else
      @sincedb_path = Pathname.new(@sincedb_path)
    end

    # This section is going to be deprecated eventually, as path.data will be
    # the default, not an environment variable (SINCEDB_DIR or HOME)
    if @sincedb_path.nil? # If it is _still_ nil...
      if ENV["SINCEDB_DIR"].nil? && ENV["HOME"].nil?
        @logger.error("No SINCEDB_DIR or HOME environment variable set, I don't know where " \
                      "to keep track of the files I'm watching. Either set " \
                      "HOME or SINCEDB_DIR in your environment, or set sincedb_path in " \
                      "in your Logstash config for the file input with " \
                      "path '#{@path.inspect}'")
        raise # TODO(sissel): HOW DO I FAIL PROPERLY YO
      end

      #pick SINCEDB_DIR if available, otherwise use HOME
      sincedb_dir = Pathname.new(ENV["SINCEDB_DIR"] || ENV["HOME"])

      # Join by ',' to make it easy for folks to know their own sincedb
      # generated path (vs, say, inspecting the @path array)
      @sincedb_path = sincedb_dir.join(".sincedb_" + Digest::MD5.hexdigest(@path.join(",")))

      # Migrate any old .sincedb to the new file (this is for version <=1.1.1 compatibility)
      old_sincedb = sincedb_dir.join(".sincedb")
      if old_sincedb.exists?
        @logger.debug("Renaming old ~/.sincedb to new one", :old => old_sincedb.to_s,
                     :new => @sincedb_path.to_s)
        old_sincedb.rename(@sincedb_path)
      end

      @logger.info("No sincedb_path set, generating one based on the file path",
                   :sincedb_path => @sincedb_path.to_s, :path => @path)
    end

    if @sincedb_path.directory?
      raise ArgumentError.new("The \"sincedb_path\" argument must point to a file, received a directory: \"#{@sincedb_path}\"")
    end

    @filewatch_config[:sincedb_path] = @sincedb_path

    @filewatch_config[:start_new_files_at] = @start_position.to_sym

    if @file_completed_action == 'log'
      if @file_completed_log_path.nil?
        raise ArgumentError.new('The "file_completed_log_path" setting must be provided when the "file_completed_action" is set to "log"')
      else
        @file_completed_log_path = Pathname.new(@file_completed_log_path)
        unless @file_completed_log_path.exist?
          begin
            FileUtils.touch(@file_completed_log_path)
          rescue
            raise ArgumentError.new("The \"file_completed_log_path\" file can't be created: #{@file_completed_log_path}")
          end
        end
      end
    end

    if tail_mode?
      require "filewatch/observing_tail"
      @watcher_class = FileWatch::ObservingTail
    else
      require "filewatch/observing_read"
      @watcher_class = FileWatch::ObservingRead
      if @file_completed_action == 'delete'
        @completed_file_handler = DeleteCompletedFileHandler.new
      elsif @file_completed_action == 'log'
        @completed_file_handler = LogCompletedFileHandler.new(@file_completed_log_path)
      else
        # nothing
      end
    end
    @codec = LogStash::Codecs::IdentityMapCodec.new(@codec)
  end # def register

  def listener_for(path)
    # path is the identity
    FileListener.new(path, self)
  end

  def start_processing
    # if the pipeline restarts this input,
    # make sure previous files are closed
    stop
    @watcher = @watcher_class.new(@filewatch_config)
    @path.each { |path| @watcher.watch_this(path) }
  end

  def run(queue)
    start_processing
    @queue = queue
    @watcher.subscribe(self) # halts here until quit is called
    exit_flush
  end # def run

  def post_process_this(event)
    event.set("[@metadata][host]", @host)
    event.set("host", @host) unless event.include?("host")
    decorate(event)
    @queue << event
  end

  def handle_deletable_path(path)
    return if tail_mode?
    return if @completed_file_handler.nil?
    @completed_file_handler.handle(path)
    # event = LogStash::Event.new
    # event.set("[@metadata][path]", path)
    # event.set("path", path)
    # event.tag("file_input_read_mode_path_deletable")
    # @queue << event
  end

  def log_line_received(path, line)
    return unless @logger.debug?
    @logger.debug("Received line", :path => path, :text => line)
  end

  def stop
    # in filewatch >= 0.6.7, quit will closes and forget all files
    # but it will write their last read positions to since_db
    # beforehand
    if @watcher
      @codec.close
      @watcher.quit
    end
  end

  private

  def tail_mode?
    @mode == "tail"
  end

  def read_mode?
    !tail_mode?
  end

  def exit_flush
    listener = FlushableListener.new("none", self)
    if @codec.identity_count.zero?
      # using the base codec without identity/path info
      @codec.base_codec.flush do |event|
        begin
          listener.process_event(event)
        rescue => e
          @logger.error("File Input: flush on exit downstream error", :exception => e)
        end
      end
    else
      @codec.flush_mapped(listener)
    end
  end
end end end# class LogStash::Inputs::File
