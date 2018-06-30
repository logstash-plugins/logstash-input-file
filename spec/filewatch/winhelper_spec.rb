# encoding: utf-8
require "stud/temporary"
require "fileutils"

if Gem.win_platform?
  require "filewatch/winhelper"

  describe Winhelper do
    let(:path) { Stud::Temporary.file.path }

    after do
      FileUtils.rm_rf(path)
    end

    it "return a unique file identifier" do
      identifier = Winhelper.identifier_from_path(path)

      expect(identifier).not_to eq("unknown")
      expect(identifier.count("-")).to eq(2)
    end
  end
end
