# encoding: utf-8

module FileWatch module Stat
  class Generic

    attr_reader :inode, :modified_at, :size, :inode_struct

    def initialize(source)
      @source = source # Pathname
      restat
    end

    def restat
      stat = @source.stat
      @inode = stat.ino.to_s
      @modified_at = stat.mtime.to_f
      @size = stat.size
      @inode_struct = InodeStruct.new(@inode, stat.dev_major, stat.dev_minor)
    end

    def windows?
      false
    end

    def inspect
      "<#{self.class.name} size=#{@size}, modified_at=#{@modified_at}, inode='#{@inode}', inode_struct=#{@inode_struct}>"
    end
  end
end end
