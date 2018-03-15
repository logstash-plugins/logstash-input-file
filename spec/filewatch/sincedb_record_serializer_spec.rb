# encoding: utf-8
require_relative 'spec_helper'
require 'filewatch/bootstrap'


module FileWatch
  describe SincedbRecordSerializer do
    let(:opts) { Hash.new }
    let(:io) { StringIO.new }
    let(:db) { Hash.new }
    before do
      OPTS.add_settings(opts)
    end
    context "deserialize from IO" do
      it 'reads V1 records' do
        # io.write("5391299 1 4 12 1520966554.91894 /a/path/to/1.log\n")
        io.write("5391299 1 4 12\n")
        SincedbRecordSerializer.deserialize(io) do |inode_struct, sincedb_value|
          expect(inode_struct.inode).to eq("5391299")
          expect(inode_struct.maj).to eq(1)
          expect(inode_struct.min).to eq(4)
          expect(sincedb_value.position).to eq(12)
        end
      end

      it 'reads V2 records from an IO object' do
        now = Time.now.to_f
        io.write("5391299 1 4 12 #{now} /a/path/to/1.log\n")
        SincedbRecordSerializer.deserialize(io) do |inode_struct, sincedb_value|
          expect(inode_struct.inode).to eq("5391299")
          expect(inode_struct.maj).to eq(1)
          expect(inode_struct.min).to eq(4)
          expect(sincedb_value.position).to eq(12)
          expect(sincedb_value.last_changed_at).to eq(now)
          expect(sincedb_value.path_in_sincedb).to eq("/a/path/to/1.log")
        end
      end
    end

    context "serialize to IO" do
      it "writes db entries" do
        now = Time.now.to_f
        inode_struct = InodeStruct.new("42424242", 2, 5)
        sincedb_value = SincedbValue.new(42, now)
        # expect(sincedb_value.last_changed_at_expires).to eq(now)
        db[inode_struct] = sincedb_value
        SincedbRecordSerializer.serialize(db, io)
        expect(io.string).to eq("42424242 2 5 42 #{now}\n")
      end

      it "does not write expired db entries to an IO object" do
        twelve_days_ago = Time.now.to_f - (12.0*24*3600)
        sixteen_days_ago = twelve_days_ago - (4.0*24*3600)
        db[InodeStruct.new("42424242", 2, 5)] = SincedbValue.new(42, twelve_days_ago)
        db[InodeStruct.new("18181818", 1, 6)] = SincedbValue.new(99, sixteen_days_ago)
        SincedbRecordSerializer.serialize(db, io)
        expect(io.string).to eq("42424242 2 5 42 #{twelve_days_ago}\n")
      end
    end

    context "given a non default `sincedb_clean_after`" do
      let(:opts) { super.merge(:sincedb_clean_after => 2)}
      it "does not write expired db entries to an IO object" do
        one_day_ago = Time.now.to_f - (1.0*24*3600)
        three_days_ago = one_day_ago - (2.0*24*3600)
        db[InodeStruct.new("42424242", 2, 5)] = SincedbValue.new(42, one_day_ago)
        db[InodeStruct.new("18181818", 1, 6)] = SincedbValue.new(99, three_days_ago)
        SincedbRecordSerializer.serialize(db, io)
        expect(io.string).to eq("42424242 2 5 42 #{one_day_ago}\n")
      end
    end
  end
end