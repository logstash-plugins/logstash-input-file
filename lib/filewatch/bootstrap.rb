# encoding: utf-8
require "pathname"

## Common setup
#  all the required constants and files
#  defined in one place
module FileWatch
  # the number of bytes read from a file during the read phase
  FILE_READ_SIZE = 32768
  # the largest fixnum in ruby
  # this is used in the read loop e.g.
  # @opts[:file_chunk_count].times do
  # where file_chunk_count defaults to this constant
  FIXNUM_MAX = (2**(0.size * 8 - 2) - 1)

  require_relative "helper"

  module WindowsInode
    def prepare_inode(path, stat)
      fileId = Winhelper.identifier_from_path(path)
      [fileId, 0, 0] # dev_* doesn't make sense on Windows
    end
  end

  module UnixInode
    def prepare_inode(path, stat)
      [stat.ino.to_s, stat.dev_major, stat.dev_minor]
    end
  end

  jar_version = Pathname.new(__FILE__).dirname.join("../../JAR_VERSION").realpath.read.strip

  require "java"
  fullpath = Pathname.new("lib/jars/filewatch-#{jar_version}.jar").expand_path.to_path
  require fullpath
  require "jruby_file_watch"

  if LogStash::Environment.windows?
    require_relative "winhelper"
    FileOpener = FileExt
    InodeMixin = WindowsInode
  else
    FileOpener = ::File
    InodeMixin = UnixInode
  end

  # Structs can be used as hash keys because they compare by value
  # this is used as the key for values in the sincedb hash
  InodeStruct = Struct.new(:inode, :maj, :min) do
    def to_s
      to_a.join(" ")
    end
  end

  BufferExtractResult = Struct.new(:lines, :warning, :additional)

  class NoSinceDBPathGiven < StandardError; end

  # how often (in seconds) we logger.warn a failed file open, per path.
  OPEN_WARN_INTERVAL = ENV.fetch("FILEWATCH_OPEN_WARN_INTERVAL", 300).to_i
  MAX_FILES_WARN_INTERVAL = ENV.fetch("FILEWATCH_MAX_FILES_WARN_INTERVAL", 20).to_i

  require "logstash/util/buftok"
  require_relative "settings"
  require_relative "sincedb_value"
  require_relative "sincedb_record_serializer"
  require_relative "watched_files_collection"
  require_relative "sincedb_collection"
  require_relative "watch"
  require_relative "watched_file"
  require_relative "discoverer"
  require_relative "observing_base"
  require_relative "observing_tail"
  require_relative "observing_read"
end
