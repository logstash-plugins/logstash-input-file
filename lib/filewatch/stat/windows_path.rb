# encoding: utf-8
require "logstash/util/loggable"

module FileWatch module Stat
  class WindowsPath
    include LogStash::Util::Loggable

    attr_reader :inode, :modified_at, :size, :inode_struct

    def initialize(source)
      @source = source # Pathname
      @inode = nil
      restat
    end

    def restat
      stat = @source.stat
      identifier = Winhelper.identifier_from_path(@source.to_path)
      if @inode.nil?
        if identifier == 'unknown'
          logger.debug("restat: file identifier lookup failed, rotation will not be detected for this file", :path => @source.to_path)
        end
        set_identifier(identifier)
      elsif @inode == 'unknown'
        # stays unknown
      elsif identifier == 'unknown'
        logger.debug("restat: file identifier lookup failed, keeping previous stat", :path => @source.to_path, :identifier => @inode)
        return
      elsif identifier != @inode
        logger.debug("restat: file identifier changed", :path => @source.to_path, :old_identifier => @inode, :new_identifier => identifier)
        set_identifier(identifier)
      end
      @modified_at = stat.mtime.to_f
      @size = stat.size
    end

    def windows?
      true
    end

    def inspect
      "<#{self.class.name} size=#{@size}, modified_at=#{@modified_at}, inode=#{@inode}, inode_struct=#{@inode_struct}>"
    end

    private

    def set_identifier(identifier)
      @inode = identifier
      # in windows the dev hi and low are in the identifier
      @inode_struct = InodeStruct.new(@inode, 0, 0)
    end
  end
end end
