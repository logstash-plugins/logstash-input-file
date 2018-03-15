# encoding: utf-8

module FileWatch
  class Settings
    attr_reader :delimiter, :close_older, :ignore_older, :delimiter_byte_size
    attr_reader :max_active, :max_warn_msg, :lastwarn_max_files
    attr_reader :sincedb_write_interval, :stat_interval, :discover_interval
    attr_reader :exclude, :start_new_files_at, :read_iterations, :file_chunk_size
    attr_reader :sincedb_path, :sincedb_write_interval, :sincedb_expiry_duration

    def initialize
      @opts = {}
    end

    def add_settings(opts)
      @opts = opts
      @max_active = 4095
      @max_warn_msg = "Reached open files limit: 4095, set by the 'max_open_files' option or default"
      self.max_open_files = opts.fetch(:max_active, ENV["FILEWATCH_MAX_OPEN_FILES"].to_i)
      @lastwarn_max_files = 0
      @delimiter = opts.fetch(:delimiter, "\n")
      @file_chunk_size = opts.fetch(:file_chunk_size, FILE_READ_SIZE)
      @delimiter_byte_size = @delimiter.bytesize
      @close_older = opts[:close_older]
      @ignore_older = opts[:ignore_older]
      @sincedb_write_interval = opts[:sincedb_write_interval]
      @stat_interval = opts[:stat_interval]
      @discover_interval = opts[:discover_interval]
      @exclude = opts[:exclude]
      @start_new_files_at = opts[:start_new_files_at]
      @read_iterations = opts.fetch(:read_iterations, FIXNUM_MAX)
      @sincedb_path = opts[:sincedb_path]
      @sincedb_write_interval = opts[:sincedb_write_interval]
      @sincedb_expiry_duration = opts.fetch(:sincedb_clean_after, 14).to_f * (24 * 3600)
    end

    def max_open_files=(value)
      val = value.to_i
      val = 4095 if value.nil? || val <= 0
      @max_warn_msg = "Reached open files limit: #{val}, set by the 'max_open_files' option or default"
      @max_active = val
    end
  end
end
