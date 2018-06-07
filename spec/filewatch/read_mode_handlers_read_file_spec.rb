# encoding: utf-8
require_relative 'spec_helper'

module FileWatch
  describe ReadMode::Handlers::ReadFile do
    let(:settings) do
      Settings.from_options(
        :sincedb_write_interval => 0,
        :sincedb_path => File::NULL
      )
    end
    let(:sdb_collection) { SincedbCollection.new(settings) }
    let(:directory) { Pathname.new(FIXTURE_DIR) }
    let(:pathname) { directory.join('uncompressed.log') }
    let(:watched_file) { WatchedFile.new(pathname, pathname.stat, settings) }
    let(:processor) { ReadMode::Processor.new(settings).add_watch(watch) }
    let(:expected_sincedb_write_call_count) { 3 }
    let(:file) { DummyFileReader.new(settings.file_chunk_size, expected_sincedb_write_call_count - 1) }

    context "simulate reading a 64KB file with a default chunk size of 32KB and a zero sincedb write interval" do
      let(:watch) { double("watch", :quit? => false) }
      it "calls 'sincedb_write' exactly 3 times" do
        allow(FileOpener).to receive(:open).with(watched_file.path).and_return(file)
        expect(sdb_collection).to receive(:sincedb_write).exactly(expected_sincedb_write_call_count).times
        watched_file.activate
        processor.initialize_handlers(sdb_collection, TestObserver.new)
        processor.read_file(watched_file)
      end
    end

    context "simulate reading a 64KB file with a default chunk size of 32KB and a zero sincedb write interval" do
      let(:watch) { double("watch", :quit? => true) }
      it "calls 'sincedb_write' exactly 0 times as shutdown is in progress" do
        expect(sdb_collection).to receive(:sincedb_write).exactly(0).times
        watched_file.activate
        processor.initialize_handlers(sdb_collection, TestObserver.new)
        processor.read_file(watched_file)
      end
    end
  end
end
