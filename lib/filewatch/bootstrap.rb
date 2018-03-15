# encoding: utf-8
require "rbconfig"
require "pathname"

## Common setup
#  all the required constants and files
#  defined in one place
module FileWatch
  HOST_OS_WINDOWS = (RbConfig::CONFIG['host_os'] =~ /mswin|mingw|cygwin/) != nil
  # the number of bytes read from a file during the read phase
  FILE_READ_SIZE = 32768
  # each sincedb record will expire unless it is seen again
  # this is the number of days a record needs
  # to be stale before it is considered gone
  SDB_EXPIRES_DAYS = 10
  # the largest fixnum in ruby
  # this is used in the read loop e.g.
  # @opts[:read_iterations].times do
  # where read_iterations defaults to this constant
  FIXNUM_MAX = (2**(0.size * 8 - 2) - 1)

  require_relative "helper"

  if HOST_OS_WINDOWS
    require "winhelper"
    FILEWATCH_INODE_METHOD = :win_inode
  else
    FILEWATCH_INODE_METHOD = :nix_inode
  end

  if defined?(JRUBY_VERSION)
    require "java"
    require_relative "../../lib/jars/filewatch-1.0.0.jar"
    require "jruby_file_watch"
  end

  if HOST_OS_WINDOWS && defined?(JRUBY_VERSION)
    FileOpener = FileExt
  else
    FileOpener = ::File
  end

  # Structs can be used as hash keys because they compare by value
  # this is used as the key for values in the sincedb hash
  InodeStruct = Struct.new(:inode, :maj, :min) do
    def to_s
      to_a.join(" ")
    end
  end

  class NoSinceDBPathGiven < StandardError; end

  # how often (in seconds) we logger.warn a failed file open, per path.
  OPEN_WARN_INTERVAL = ENV.fetch("FILEWATCH_OPEN_WARN_INTERVAL", 300).to_i
  MAX_FILES_WARN_INTERVAL = ENV.fetch("FILEWATCH_MAX_FILES_WARN_INTERVAL", 20).to_i

  require_relative "settings"
  OPTS = Settings.new
  require_relative "buftok"
  require_relative "sincedb_value"
  require_relative "sincedb_record_serializer"
  require_relative "watched_files_collection"
  require_relative "sincedb_collection"
  require_relative "watch"
  require_relative "watched_file"
  require_relative "discoverer"
  require_relative "observing_base"
  require_relative "tail_handlers/base"
  require_relative "read_handlers/base"

  # TODO [guy] make this a config option, perhaps.
  CurrentSerializer = SincedbRecordSerializer

  # these classes are used if the caller does not
  # supply their own observer and listener
  # which would be a programming error when coding against
  # observable_tail
  class NullListener
    def initialize(path) @path = path; end
    def accept(line) end
    def deleted
    end
    def opened
    end
    def error
    end
    def eof
    end
    def timed_out
    end
  end

  class NullObserver
    def listener_for(path) NullListener.new(path); end
  end
end
