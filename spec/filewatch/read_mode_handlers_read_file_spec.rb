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
    let(:watched_file) { WatchedFile.new(pathname, PathStatClass.new(pathname), settings) }
    let(:processor) { ReadMode::Processor.new(settings).add_watch(watch) }
    let(:file) { DummyFileReader.new(settings.file_chunk_size, 2) }

    context "simulate reading a 64KB file with a default chunk size of 32KB and a zero sincedb write interval" do
      let(:watch) { double("watch", :quit? => false) }
      it "calls 'sincedb_write' exactly 2 times" do
        allow(FileOpener).to receive(:open).with(watched_file.path).and_return(file)
        expect(sdb_collection).to receive(:sincedb_write).exactly(1).times
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

    context "when restart from existing sincedb" do
      let(:settings) do
        Settings.from_options(
          :sincedb_write_interval => 0,
          :sincedb_path => File::NULL,
          :file_chunk_size => 10
        )
      end

      let(:processor) { double("fake processor") }
      let(:observer) { TestObserver.new }
      let(:watch) { double("watch") }

      before(:each) {
        allow(watch).to receive(:quit?).and_return(false)#.and_return(false).and_return(true)
        allow(processor).to receive(:watch).and_return(watch)
      }

      it "read from where it left" do
        listener = observer.listener_for(Pathname.new(pathname).to_path)
        sut = ReadMode::Handlers::ReadFile.new(processor, sdb_collection, observer, settings)

        # simulate a previous partial read of the file
        sincedb_value = SincedbValue.new(0)
        sincedb_value.set_watched_file(watched_file)
        sdb_collection.set(watched_file.sincedb_key, sincedb_value)


        # simulate a consumption of first line, (size + newline) bytes
        sdb_collection.increment(watched_file.sincedb_key, File.readlines(pathname)[0].size + 2)

        # exercise
        sut.handle(watched_file)

        # verify
        expect(listener.lines.size).to eq(1)
        expect(listener.lines[0]).to start_with("2010-03-12   23:51:21   SEA4   192.0.2.222   play   3914   OK")
      end
    end
  end
end
