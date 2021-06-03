# encoding: utf-8
require "logstash/util/loggable"

module FileWatch
  # this KV collection has a watched_file storage_key (an InodeStruct) as the key
  # and a SincedbValue as the value.
  # the SincedbValues are built by reading the sincedb file.
  class SincedbCollection
    include LogStash::Util::Loggable

    attr_reader :path
    attr_writer :serializer

    def initialize(settings)
      @settings = settings
      @sincedb_last_write = 0
      @sincedb = {}
      @serializer = SincedbRecordSerializer.new(@settings.sincedb_expiry_duration)
      @path = Pathname.new(@settings.sincedb_path)
      @write_method = LogStash::Environment.windows? || @path.chardev? || @path.blockdev? ? method(:non_atomic_write) : method(:atomic_write)
      @full_path = @path.to_path
      FileUtils.touch(@full_path)
      @write_requested = false
    end

    def write_requested?
      @write_requested
    end

    def request_disk_flush
      @write_requested = true
      flush_at_interval
    end

    def write_if_requested
      if write_requested?
        flush_at_interval
      end
    end

    def write(reason=nil)
      logger.trace("caller requested sincedb write (#{reason})")
      sincedb_write
    end

    def open
      @time_sdb_opened = Time.now.to_f
      begin
        path.open do |file|
          logger.debug("open: reading from #{path}")
          @serializer.deserialize(file) do |key, value|
            logger.trace? && logger.trace("open: importing #{key.inspect} => #{value.inspect}")
            set_key_value(key, value)
          end
        end
        logger.trace("open: count of keys read: #{@sincedb.keys.size}")
      rescue => e
        #No existing sincedb to load
        logger.debug("open: error opening #{path}", :exception => e.class, :message => e.message)
      end
    end

    def associate(watched_file)
      logger.trace? && logger.trace("associate: finding", :path => watched_file.path, :inode => watched_file.sincedb_key.inode)
      sincedb_value = find(watched_file)
      if sincedb_value.nil?
        # sincedb has no record of this inode
        # and due to the window handling of many files
        # this file may not be opened in this session.
        # a new value will be added when the file is opened
        logger.trace("associate: unmatched", :filename => watched_file.filename)
        return true
      end
      logger.trace? && logger.trace("associate: found sincedb record", :filename => watched_file.filename,
                                    :sincedb_key => watched_file.sincedb_key, :sincedb_value => sincedb_value)
      if sincedb_value.watched_file.nil? # not associated
        if sincedb_value.path_in_sincedb.nil?
          handle_association(sincedb_value, watched_file)
          logger.trace? && logger.trace("associate: inode matched but no path in sincedb", :filename => watched_file.filename)
          return true
        end
        if sincedb_value.path_in_sincedb == watched_file.path
          # the path on disk is the same as discovered path and the inode is the same.
          handle_association(sincedb_value, watched_file)
          logger.trace? && logger.trace("associate: inode and path matched", :filename => watched_file.filename)
          return true
        end
        # the path on disk is different from discovered unassociated path but they have the same key (inode)
        # treat as a new file, a new value will be added when the file is opened
        sincedb_value.clear_watched_file
        delete(watched_file.sincedb_key)
        logger.trace? && logger.trace("associate: matched but allocated to another", :filename => watched_file.filename)
        return true
      end
      if sincedb_value.watched_file.equal?(watched_file) # pointer equals
        logger.trace? && logger.trace("associate: already associated", :filename => watched_file.filename)
        return true
      end
      # sincedb_value.watched_file is not this discovered watched_file but they have the same key (inode)
      # this means that the filename path was changed during this session.
      # renamed file can be discovered...
      #   before the original is detected as deleted: state is `active`
      #   after the original is detected as deleted but before it is actually deleted: state is `delayed_delete`
      #   after the original is deleted
      # are not yet in the delete phase, let this play out
      existing_watched_file = sincedb_value.watched_file
      logger.trace? && logger.trace("associate: found sincedb_value has a watched_file - this is a rename",
                                    :this_watched_file => watched_file.details, :existing_watched_file => existing_watched_file.details)
      watched_file.rotation_in_progress
      true
    end

    def find(watched_file)
      get(watched_file.sincedb_key)
    end

    def member?(key)
      @sincedb.member?(key)
    end

    def get(key)
      @sincedb[key]
    end

    def set(key, value)
      @sincedb[key] = value
      value
    end

    def delete(key)
      @sincedb.delete(key)
    end

    def last_read(key)
      @sincedb[key].position
    end

    def rewind(key)
      @sincedb[key].update_position(0)
    end

    def increment(key, amount)
      @sincedb[key].increment_position(amount)
    end

    def set_watched_file(key, watched_file)
      @sincedb[key].set_watched_file(watched_file)
    end

    def watched_file_deleted(watched_file)
      value = @sincedb[watched_file.sincedb_key]
      value.unset_watched_file if value
    end

    def store_last_read(key, pos)
      @sincedb[key].update_position(pos)
    end

    def clear_watched_file(key)
      @sincedb[key].clear_watched_file
    end

    def reading_completed(key)
      @sincedb[key].reading_completed
    end

    def clear
      @sincedb.clear
    end

    def keys
      @sincedb.keys
    end

    def watched_file_unset?(key)
      return false unless member?(key)
      get(key).watched_file.nil?
    end

    def flush_at_interval
      now = Time.now
      delta = now.to_i - @sincedb_last_write
      if delta >= @settings.sincedb_write_interval
        logger.debug("writing sincedb (delta since last write = #{delta})")
        sincedb_write(now)
      end
    end

    private

    def handle_association(sincedb_value, watched_file)
      watched_file.update_bytes_read(sincedb_value.position)
      sincedb_value.set_watched_file(watched_file)
      watched_file.initial_completed
      if watched_file.all_read?
        watched_file.ignore
        logger.trace? && logger.trace("handle_association fully read, ignoring", :watched_file => watched_file.details, :sincedb_value => sincedb_value)
      end
    end

    def set_key_value(key, value)
      if @time_sdb_opened < value.last_changed_at_expires(@settings.sincedb_expiry_duration)
        set(key, value)
      else
        logger.debug("set_key_value: record has expired, skipping: #{key.inspect} => #{value.inspect}")
      end
    end

    def sincedb_write(time = Time.now)
      logger.trace? && logger.trace("sincedb_write: #{path} (time = #{time})")
      begin
        expired_keys = @write_method.call(time)
        expired_keys.each do |key|
          @sincedb[key].unset_watched_file
          delete(key)
          logger.trace? && logger.trace("sincedb_write: cleaned", :key => key)
        end
        @sincedb_last_write = time.to_i
        @write_requested = false
      rescue Errno::EACCES => e
        # no file handles free perhaps - maybe it will work next time
        logger.debug("sincedb_write: #{path} error:", :exception => e.class, :message => e.message)
      end
    end

    # @return expired keys
    def atomic_write(time)
      logger.trace? && logger.trace("non_atomic_write: ", :time => time)
      begin
        FileHelper.write_atomically(@full_path) do |io|
          @serializer.serialize(@sincedb, io, time.to_f)
        end
      rescue Errno::EPERM, Errno::EACCES => e
        logger.warn("sincedb_write: unable to write atomically due to permissions error, falling back to non-atomic write: #{path} error:", :exception => e.class, :message => e.message)
        @write_method = method(:non_atomic_write)
        non_atomic_write(time)
      rescue => e
        logger.warn("sincedb_write: unable to write atomically, attempting non-atomic write: #{path} error:", :exception => e.class, :message => e.message)
        non_atomic_write(time)
      end
    end

    # @return expired keys
    def non_atomic_write(time)
      logger.trace? && logger.trace("non_atomic_write: ", :time => time)
      File.open(@full_path, "w+") do |io|
        @serializer.serialize(@sincedb, io, time.to_f)
      end
    end
  end
end
