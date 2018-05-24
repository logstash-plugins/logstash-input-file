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
      logger.debug("caller requested sincedb write (#{reason})")
      sincedb_write
    end

    def open
      @time_sdb_opened = Time.now.to_f
      begin
        path.open do |file|
          logger.debug("open: reading from #{path}")
          @serializer.deserialize(file) do |key, value|
            logger.debug("open: importing ... '#{key}' => '#{value}'")
            set_key_value(key, value)
          end
        end
        logger.debug("open: count of keys read: #{@sincedb.keys.size}")
      rescue => e
        #No existing sincedb to load
        logger.debug("open: error: #{path}: #{e.inspect}")
      end
    end

    def associate(watched_file)
      logger.debug("associate: finding", "inode" => watched_file.sincedb_key.inode, "path" => watched_file.path)
      sincedb_value = find(watched_file)
      if sincedb_value.nil?
        # sincedb has no record of this inode
        # and due to the window handling of many files
        # this file may not be opened in this session.
        # a new value will be added when the file is opened
        logger.debug("associate: unmatched")
        return
      end
      if sincedb_value.watched_file.nil?
        # not associated
        if sincedb_value.path_in_sincedb.nil?
          # old v1 record, assume its the same file
          handle_association(sincedb_value, watched_file)
          logger.debug("associate: inode matched but no path in sincedb")
          return
        end
        if sincedb_value.path_in_sincedb == watched_file.path
          # the path on disk is the same as discovered path
          # and the inode is the same.
          handle_association(sincedb_value, watched_file)
          logger.debug("associate: inode and path matched")
          return
        end
        # the path on disk is different from discovered unassociated path
        # but they have the same key (inode)
        # treat as a new file, a new value will be added when the file is opened
        sincedb_value.clear_watched_file
        delete(watched_file.sincedb_key)
        logger.debug("associate: matched but allocated to another - #{sincedb_value}")
        return
      end
      if sincedb_value.watched_file.equal?(watched_file) # pointer equals
        logger.debug("associate: already associated - #{sincedb_value}, for path: #{watched_file.path}")
        return
      end
      # sincedb_value.watched_file is not this discovered watched_file but they have the same key (inode)
      # this means that the filename path was changed during this session.
      # renamed file can be discovered...
      #   before the original is detected as deleted: state is `active`
      #   after the original is detected as deleted but before it is actually deleted: state is `delayed_delete`
      #   after the original is deleted
      # are not yet in the delete phase, let this play out
      old_watched_file = sincedb_value.watched_file
      if old_watched_file.file_open?
        msg = "associate: the found sincedb_value has a watched_file with an open file handle - this is a rename, switching to discovered file"
        logger.debug(msg,
        "discovered watched_file state" => watched_file.state,
        "discovered watched_file size" => watched_file.last_stat_size,
        "discovered watched_file bytes read" => watched_file.bytes_read,
        "discovered watched_file path" => watched_file.path,
        "found watched_file state" => old_watched_file.state,
        "found watched_file size" => old_watched_file.last_stat_size,
        "found watched_file bytes read" => old_watched_file.bytes_read,
        "found watched_file path" => old_watched_file.path
        )
        sincedb_value.set_watched_file(watched_file)
        sincedb_value.update_position(old_watched_file.bytes_read)
        watched_file.initial_completed
        return
      end
      # logout the history of both watched_files then remove the sincdb entry.
      # a new value will be added when the file is processed
      sincedb_value.clear_watched_file
      msg = "associate: matched but allocated to another watched_file - DELETING value at key. Was this file renamed before it was processed?"
      logger.warn(msg,
        "sincedb_key" => old_watched_file.sincedb_key,
        "sincedb_value" => sincedb_value,
        "discovered watched_file history" => watched_file.recent_state_history.join(', '),
        "discovered watched_file size" => watched_file.last_stat_size,
        "discovered watched_file bytes read" => watched_file.bytes_read,
        "discovered watched_file bytes unread" => watched_file.bytes_unread,
        "discovered watched_file path" => watched_file.path,
        "found watched_file history" => old_watched_file.recent_state_history.join(', '),
        "found watched_file size" => old_watched_file.last_stat_size,
        "found watched_file bytes read" => old_watched_file.bytes_read,
        "found watched_file bytes unread" => old_watched_file.bytes_unread,
        "found watched_file path" => old_watched_file.path
      )
      # the risk in deleting the sincedb record here is:
      # 1. if the file is unread - none, a new record is created and the file is read.
      # 2. if the inode was reused - none, a new record is created and the new content is read.
      # 3. if the file has been partially read and the sincedb record holds that position - data duplication.
      # we can't tell if we are in 2 or 3.
      # It is less likely that a file is renamed into the glob and then appended-to, inode reuse is more likely.
      delete(old_watched_file.sincedb_key)
    end

    def find(watched_file)
      get(watched_file.sincedb_key).tap do |obj|
        logger.debug("find: found = '#{!obj.nil?}', path: #{watched_file.path}")
      end
    end

    def member?(key)
      @sincedb.member?(key)
    end

    def get(key)
      @sincedb[key]
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

    def unset_watched_file(watched_file)
      return unless member?(watched_file.sincedb_key)
      get(watched_file.sincedb_key).unset_watched_file
    end

    def clear
      @sincedb.clear
    end

    def keys
      @sincedb.keys
    end

    def set(key, value)
      @sincedb[key] = value
      value
    end

    def watched_file_unset?(key)
      return false unless member?(key)
      get(key).watched_file.nil?
    end

    private

    def flush_at_interval
      now = Time.now.to_i
      delta = now - @sincedb_last_write
      if delta >= @settings.sincedb_write_interval
        logger.debug("writing sincedb (delta since last write = #{delta})")
        sincedb_write(now)
      end
    end

    def handle_association(sincedb_value, watched_file)
      watched_file.update_bytes_read(sincedb_value.position)
      sincedb_value.set_watched_file(watched_file)
      watched_file.initial_completed
      watched_file.ignore if watched_file.all_read?
    end

    def set_key_value(key, value)
      if @time_sdb_opened < value.last_changed_at_expires(@settings.sincedb_expiry_duration)
        logger.debug("open: setting #{key.inspect} to #{value.inspect}")
        set(key, value)
      else
        logger.debug("open: record has expired, skipping: #{key.inspect} #{value.inspect}")
      end
    end

    def sincedb_write(time = Time.now.to_i)
      logger.debug("sincedb_write: to: #{path}")
      begin
        @write_method.call
        @serializer.expired_keys.each do |key|
          @sincedb[key].unset_watched_file
          delete(key)
          logger.debug("sincedb_write: cleaned", "key" => "'#{key}'")
        end
        @sincedb_last_write = time
        @write_requested = false
      rescue Errno::EACCES
        # no file handles free perhaps
        # maybe it will work next time
        logger.debug("sincedb_write: error: #{path}: #{$!}")
      end
    end

    def atomic_write
      FileHelper.write_atomically(@full_path) do |io|
        @serializer.serialize(@sincedb, io)
      end
    end

    def non_atomic_write
      IO.open(IO.sysopen(@full_path, "w+")) do |io|
        @serializer.serialize(@sincedb, io)
      end
    end
  end
end
