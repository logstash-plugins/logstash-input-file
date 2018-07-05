# encoding: utf-8

require_relative '../filewatch/spec_helper'

if LogStash::Environment.windows?
  describe "basic ops" do
    let(:fixture_dir) { Pathname.new(FileWatch::FIXTURE_DIR).expand_path }
    let(:file_path) { fixture_dir.join('uncompressed.log') }
    it "path works" do
      path = file_path.to_path
      identifier = Winhelper.identifier_from_path(path)
      STDOUT.puts("--- >>", identifier, "------")
      expect(identifier.count('-')).to eq(2)
      fs_name = Winhelper.file_system_type_from_path(path)
      STDOUT.puts("--- >>", fs_name, "------")
      expect(fs_name).to eq("NTFS")
      # identifier = Winhelper.identifier_from_path_ex(path)
      # STDOUT.puts("--- >>", identifier, "------")
      # expect(identifier.count('-')).to eq(2)
    end

    it "io works" do
      file = FileWatch::FileOpener.open(file_path.to_path)
      identifier = Winhelper.identifier_from_io(file)
      file.close
      STDOUT.puts("--- >>", identifier, "------")
      expect(identifier.count('-')).to eq(2)
      # fs_name = Winhelper.file_system_type_from_io(file)
      # STDOUT.puts("--- >>", fs_name, "------")
      # expect(fs_name).to eq("NTFS")
      # identifier = Winhelper.identifier_from_path_ex(path)
      # STDOUT.puts("--- >>", identifier, "------")
      # expect(identifier.count('-')).to eq(2)
    end
  end
end
