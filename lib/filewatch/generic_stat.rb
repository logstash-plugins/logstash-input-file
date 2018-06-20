# encoding: utf-8

module FileWatch
  class GenericStat

    attr_reader :identifier

    def initialize(pathname, identifer = nil, io = nil)
      @source = io.nil? ? pathname : io
      @pathname = pathname
      @windows = LogStash::Environment.windows?
      if @identifier.nil?
        @identifier = @windows ? Winhelper.identifier_from_path(@pathname.to_path) : nil
      else
        @identifier = identifier
      end
      restat
    end

    def restat
      @inner_stat = @source.stat
    end

    def inode
      windows? ? @identifier : @inner_stat.ino.to_s
    end

    def modified_at
      @inner_stat.mtime.to_f
    end

    def size
      @inner_stat.size
    end

    def to_inode_struct
      InodeStruct.new(*package_struct_elements)
    end

    def windows?
      @windows
    end

    def inspect
      "<GenericStat size='#{size}', modified_at='#{modified_at}', inode='#{inode}', inode_struct='#{to_inode_struct}'>"
    end

    private

    def package_struct_elements
      windows? ? [@identifier, 0, 0] : [@inner_stat.ino.to_s, @inner_stat.dev_major, @inner_stat.dev_minor]
    end
  end
end
