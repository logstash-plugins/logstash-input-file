# encoding: utf-8
require 'stud/temporary'
require_relative 'spec_helper'
require_relative '../../lib/filewatch/stat/windows_path'

module FileWatch
  describe Stat::WindowsPath do
    let(:directory) { Stud::Temporary.directory }
    let(:file_path) { ::File.join(directory, "windows_path.log") }
    let(:pathname) { Pathname.new(file_path) }
    let(:winhelper) { double("Winhelper") }

    before do
      stub_const("Winhelper", winhelper)
      File.write(file_path, "line1\nline2\n")
    end

    after { FileUtils.rm_rf(directory) }

    it "picks up a new identifier on restat" do
      allow(winhelper).to receive(:identifier_from_path).and_return("1-2-3", "1-2-4")
      stat = described_class.new(pathname)
      File.write(file_path, "lineA\n")
      stat.restat
      expect(stat.inode).to eq("1-2-4")
      expect(stat.inode_struct).to eq(InodeStruct.new("1-2-4", 0, 0))
      expect(stat.size).to eq(6)
    end

    it "keeps the previous snapshot when the lookup fails after a successful one" do
      allow(winhelper).to receive(:identifier_from_path).and_return("1-2-3", "unknown")
      stat = described_class.new(pathname)
      File.write(file_path, "lineA\n")
      stat.restat
      expect(stat.inode).to eq("1-2-3")
      expect(stat.size).to eq(12)
    end

    it "stays unknown when the first lookup fails" do
      allow(winhelper).to receive(:identifier_from_path).and_return("unknown", "1-2-3")
      stat = described_class.new(pathname)
      File.write(file_path, "line1\nline2\nline3\n")
      stat.restat
      expect(stat.inode).to eq("unknown")
      expect(stat.size).to eq(18)
    end
  end
end
