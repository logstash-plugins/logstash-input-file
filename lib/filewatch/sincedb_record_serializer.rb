# encoding: utf-8
require_relative 'bootstrap' unless defined?(FileWatch)

module FileWatch
  module SincedbRecordSerializer
    extend self

    def serialize(db, io, as_of = Time.now.to_f)
      db.each do |key, value|
        next if as_of > value.last_changed_at_expires
        io.write(serialize_record(key, value))
      end
    end

    def deserialize(io)
      io.each do |record|
        yield deserialize_record(record)
      end
    end

    def serialize_record(k, v)
      # effectively InodeStruct#to_s SincedbValue#to_s
      "#{k} #{v}\n"
    end

    def deserialize_record(record)
      return [] if record.nil? || record.empty?
      parts = record.split(" ")
      parse_line_v2(parts) || parse_line_v1(parts)
    end

    private

    def parse_line_v2(parts)
      # new format e.g. 2977152 1 4 94 1519319662.852678 'path/to/file'
      # do we want to store the last known state of the watched file too?
      return false if parts.size < 5
      inode_struct = prepare_inode_struct(parts)
      pos = parts.shift.to_i
      expires_at = Float(parts.shift) # this is like Time.now.to_f
      path_in_sincedb = parts.shift
      value = SincedbValue.new(pos, expires_at).add_path_in_sincedb(path_in_sincedb)
      [inode_struct, value]
    end

    def parse_line_v1(parts)
      # old inode based e.g. 2977152 1 4 94
      inode_struct = prepare_inode_struct(parts)
      pos = parts.shift.to_i
      [inode_struct, SincedbValue.new(pos)]
    end

    def prepare_inode_struct(parts)
      InodeStruct.new(parts.shift, *parts.shift(2).map(&:to_i))
    end
  end
end
