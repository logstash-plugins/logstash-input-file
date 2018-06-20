# encoding: utf-8
require_relative 'spec_helper'

module FileWatch
  describe WatchedFilesCollection do
    let(:time) { Time.now }
    let(:stat1)  { double("stat1", :size => 98, :modified_at => time - 30, :identifier => nil, :inode => 234567, :to_inode_struct => InodeStruct.new("234567", 3, 2)) }
    let(:stat2)  { double("stat2", :size => 99, :modified_at => time - 20, :identifier => nil, :inode => 234568, :to_inode_struct => InodeStruct.new("234568", 3, 2)) }
    let(:stat3)  { double("stat3", :size => 100, :modified_at => time, :identifier => nil, :inode => 234569, :to_inode_struct => InodeStruct.new("234569", 3, 2)) }
    let(:wf1) { WatchedFile.new("/var/log/z.log", stat1, Settings.new) }
    let(:wf2) { WatchedFile.new("/var/log/m.log", stat2, Settings.new) }
    let(:wf3) { WatchedFile.new("/var/log/a.log", stat3, Settings.new) }

    context "sort by last_modified in ascending order" do
      let(:sort_by) { "last_modified" }
      let(:sort_direction) { "asc" }

      it "sorts earliest modified first" do
        collection = described_class.new(Settings.from_options(:file_sort_by => sort_by, :file_sort_direction => sort_direction))
        collection.add(wf2)
        expect(collection.values).to eq([wf2])
        collection.add(wf3)
        expect(collection.values).to eq([wf2, wf3])
        collection.add(wf1)
        expect(collection.values).to eq([wf1, wf2, wf3])
      end
    end

    context "sort by path in ascending order" do
      let(:sort_by) { "path" }
      let(:sort_direction) { "asc" }

      it "sorts path A-Z" do
        collection = described_class.new(Settings.from_options(:file_sort_by => sort_by, :file_sort_direction => sort_direction))
        collection.add(wf2)
        expect(collection.values).to eq([wf2])
        collection.add(wf1)
        expect(collection.values).to eq([wf2, wf1])
        collection.add(wf3)
        expect(collection.values).to eq([wf3, wf2, wf1])
      end
    end

    context "sort by last_modified in descending order" do
      let(:sort_by) { "last_modified" }
      let(:sort_direction) { "desc" }

      it "sorts latest modified first" do
        collection = described_class.new(Settings.from_options(:file_sort_by => sort_by, :file_sort_direction => sort_direction))
        collection.add(wf2)
        expect(collection.values).to eq([wf2])
        collection.add(wf1)
        expect(collection.values).to eq([wf2, wf1])
        collection.add(wf3)
        expect(collection.values).to eq([wf3, wf2, wf1])
      end
    end

    context "sort by path in descending order" do
      let(:sort_by) { "path" }
      let(:sort_direction) { "desc" }

      it "sorts path Z-A" do
        collection = described_class.new(Settings.from_options(:file_sort_by => sort_by, :file_sort_direction => sort_direction))
        collection.add(wf2)
        expect(collection.values).to eq([wf2])
        collection.add(wf1)
        expect(collection.values).to eq([wf1, wf2])
        collection.add(wf3)
        expect(collection.values).to eq([wf1, wf2, wf3])
      end
    end
  end
end
