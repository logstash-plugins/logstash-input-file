# encoding: utf-8

module FileWatch module Stat
  class WindowsIO

    attr_reader :identifier, :inode, :modified_at, :size, :inode_struct

    def initialize(source)
      @source = source
      @dev_major = 0
      @dev_minor = 0
      restat
    end

    def add_identifier(identifier)
      # when we have a more efficient `Winhelper.identifier_from_io` mechanism
      # we can use this instead like in WindowsPath
      # its a bit academic now because we open files without SHARE_DELETE in Windows
      # so files can't be renamed while they are open therefore we can't use the
      # path vs io stat change to detect a rename
      @identifier = identifier
      self
    end

    def restat
      @inner_stat = @source.stat
      # @identifier = Winhelper.identifier_from_io(@source.to_path)
      @inode = @identifier
      @modified_at = @inner_stat.mtime.to_f
      @size = @inner_stat.size
      @inode_struct = InodeStruct.new(@inode, @dev_major, @dev_minor)
    end

    def windows?
      true
    end

    def inspect
      "<WindowsPath size='#{@size}', modified_at='#{@modified_at}', inode='#{@inode}', inode_struct='#{@inode_struct}'>"
    end
  end
end end
