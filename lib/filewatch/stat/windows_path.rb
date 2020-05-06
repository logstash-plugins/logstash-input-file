# encoding: utf-8

module FileWatch module Stat
  class WindowsPath

    attr_reader :inode, :modified_at, :size, :inode_struct

    def initialize(source)
      @source = source # Pathname
      @inode = Winhelper.identifier_from_path(@source.to_path)
      # in windows the dev hi and low are in the identifier
      @inode_struct = InodeStruct.new(@inode, 0, 0)
      restat
    end

    def restat
      stat = @source.stat
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
