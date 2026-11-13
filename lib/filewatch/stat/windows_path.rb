# encoding: utf-8

module FileWatch module Stat
  class WindowsPath

    attr_reader :inode, :modified_at, :size, :inode_struct

    def initialize(source)
      @source = source # Pathname
      @inode = nil
      restat
    end

    def restat
      stat = @source.stat
      identifier = Winhelper.identifier_from_path(@source.to_path)
      if @inode.nil? || (@inode != 'unknown' && identifier != 'unknown')
        @inode = identifier
        # in windows the dev hi and low are in the identifier
        @inode_struct = InodeStruct.new(@inode, 0, 0)
      elsif @inode != 'unknown'
        return
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
  end
end end
