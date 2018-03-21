# encoding: utf-8
require_relative 'bootstrap' unless defined?(FileWatch)
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
      @serializer = CurrentSerializerClass.new(@settings.sincedb_expiry_duration)
      @path = Pathname.new(@settings.sincedb_path)
      FileUtils.touch(@path.to_path)
    end

    def request_disk_flush
      now = Time.now.to_i
      delta = now - @sincedb_last_write
      if delta >= @settings.sincedb_write_interval
        logger.debug("writing sincedb (delta since last write = #{delta})")
        sincedb_write(now)
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
      logger.debug("associate: finding: #{watched_file.path}")
      sincedb_value = find(watched_file)
      if sincedb_value.nil?
        # sincedb has no record of this inode
        # and due to the window handling of many files
        # this file may not be opened in this session.
        # a new value will be added when the file is opened
        return
      end
      if sincedb_value.watched_file.nil?
        # not associated
        if sincedb_value.path_in_sincedb.nil?
          # old v1 record, assume its the same file
          handle_association(sincedb_value, watched_file)
          return
        end
        if sincedb_value.path_in_sincedb == watched_file.path
          # the path on disk is the same as discovered path
          # and the inode is the same.
          handle_association(sincedb_value, watched_file)
          return
        end
        # the path on disk is different from discovered unassociated path
        # but they have the same key (inode)
        # treat as a new file, a new value will be added when the file is opened
        logger.debug("associate: matched but allocated to another - #{sincedb_value}")
        sincedb_value.clear_watched_file
        delete(watched_file.sincedb_key)
        return
      end
      if sincedb_value.watched_file.equal?(watched_file) # pointer equals
        logger.debug("associate: already associated - #{sincedb_value}, for path: #{watched_file.path}")
        return
      end
      # sincedb_value.watched_file is not the discovered watched_file but they have the same key (inode)
      # this means that the filename was changed during this session.
      # logout the history of the old sincedb_value and remove it
      # a new value will be added when the file is opened
      # TODO notify about done-ness of old sincedb_value and watched_file
      old_watched_file = sincedb_value.watched_file
      sincedb_value.clear_watched_file
      if logger.debug?
        logger.debug("associate: matched but allocated to another - #{sincedb_value}")
        logger.debug("associate: matched but allocated to another - old watched_file history - #{old_watched_file.recent_state_history.join(', ')}")
        logger.debug("associate: matched but allocated to another - DELETING value at key `#{old_watched_file.sincedb_key}`")
      end
      delete(old_watched_file.sincedb_key)
    end

    def find(watched_file)
      get(watched_file.sincedb_key).tap do |obj|
        logger.debug("find for path: #{watched_file.path}, found: '#{!obj.nil?}'")
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

    def store_last_read(key, last_read)
      @sincedb[key].update_position(last_read)
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
        if HOST_OS_WINDOWS || FileHelper.device?(path)
          IO.open(path, 0) do |io|
            @serializer.serialize(@sincedb, io)
          end
        else
          FileHelper.write_atomically(path) do |io|
            @serializer.serialize(@sincedb, io)
          end
        end
        @serializer.expired_keys.each do |key|
          @sincedb[key].unset_watched_file
          delete(key)
          logger.debug("sincedb_write: cleaned", "key" => "'#{key}'")
        end
        @sincedb_last_write = time
      rescue Errno::EACCES
        # no file handles free perhaps
        # maybe it will work next time
        logger.debug("sincedb_write: error: #{path}: #{$!}")
      end
    end
  end
end
