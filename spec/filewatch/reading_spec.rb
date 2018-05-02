
require 'stud/temporary'
require_relative 'spec_helper'
require 'filewatch/observing_read'

LogStash::Logging::Logger::configure_logging("WARN")

module FileWatch
  describe Watch do
    before(:all) do
      @thread_abort = Thread.abort_on_exception
      Thread.abort_on_exception = true
    end

    after(:all) do
      Thread.abort_on_exception = @thread_abort
    end

    let(:directory) { Stud::Temporary.directory }
    let(:watch_dir) { ::File.join(directory, "*.log") }
    let(:file_path) { ::File.join(directory, "1.log") }
    let(:sincedb_path) { ::File.join(Stud::Temporary.directory, "reading.sdb") }
    let(:stat_interval) { 0.1 }
    let(:discover_interval) { 4 }
    let(:start_new_files_at) { :end } # should be irrelevant for read mode
    let(:opts) do
      {
        :stat_interval => stat_interval, :start_new_files_at => start_new_files_at,
        :delimiter => "\n", :discover_interval => discover_interval,
        :ignore_older => 3600, :sincedb_path => sincedb_path
      }
    end
    let(:observer) { TestObserver.new }
    let(:reading) { ObservingRead.new(opts) }
    let(:actions) do
      RSpec::Sequencing.run_after(0.45, "quit after a short time") do
        reading.quit
      end
    end

    after do
      FileUtils.rm_rf(directory) unless directory =~ /fixture/
    end

    context "when watching a directory with files" do
      let(:directory) { Stud::Temporary.directory }
      let(:watch_dir) { ::File.join(directory, "*.log") }
      let(:file_path) { ::File.join(directory, "1.log") }

      it "the file is read" do
        File.open(file_path, "wb") { |file|  file.write("line1\nline2\n") }
        actions.activate
        reading.watch_this(watch_dir)
        reading.subscribe(observer)
        expect(observer.listener_for(file_path).calls).to eq([:open, :accept, :accept, :eof, :delete])
        expect(observer.listener_for(file_path).lines).to eq(["line1", "line2"])
      end
    end

    context "when watching a directory with files and sincedb_path is /dev/null or NUL" do
      let(:directory) { Stud::Temporary.directory }
      let(:sincedb_path) { File::NULL }
      let(:watch_dir) { ::File.join(directory, "*.log") }
      let(:file_path) { ::File.join(directory, "1.log") }

      it "the file is read" do
        File.open(file_path, "wb") { |file|  file.write("line1\nline2\n") }
        actions.activate
        reading.watch_this(watch_dir)
        reading.subscribe(observer)
        expect(observer.listener_for(file_path).calls).to eq([:open, :accept, :accept, :eof, :delete])
        expect(observer.listener_for(file_path).lines).to eq(["line1", "line2"])
      end
    end

    context "when watching a directory with files using striped reading" do
      let(:directory) { Stud::Temporary.directory }
      let(:watch_dir) { ::File.join(directory, "*.log") }
      let(:file_path1) { ::File.join(directory, "1.log") }
      let(:file_path2) { ::File.join(directory, "2.log") }
      # use a chunk size that does not align with the line boundaries
      let(:opts) { super.merge(:file_chunk_size => 10, :file_chunk_count => 1)}
      let(:lines) { [] }
      let(:observer) { TestObserver.new(lines) }

      it "the files are read seemingly in parallel" do
        File.open(file_path1, "w") { |file|  file.write("string1\nstring2\n") }
        File.open(file_path2, "w") { |file|  file.write("stringA\nstringB\n") }
        actions.activate
        reading.watch_this(watch_dir)
        reading.subscribe(observer)
        if lines.first == "stringA"
          expect(lines).to eq(%w(stringA string1 stringB string2))
        else
          expect(lines).to eq(%w(string1 stringA string2 stringB))
        end
      end
    end

    context "when a non default delimiter is specified and it is not in the content" do
      let(:opts) { super.merge(:delimiter => "\nø") }

      it "the file is opened, data is read, but no lines are found initially, at EOF the whole file becomes the line" do
        File.open(file_path, "wb") { |file|  file.write("line1\nline2") }
        actions.activate
        reading.watch_this(watch_dir)
        reading.subscribe(observer)
        listener = observer.listener_for(file_path)
        expect(listener.calls).to eq([:open, :accept, :eof, :delete])
        expect(listener.lines).to eq(["line1\nline2"])
        sincedb_record_fields = File.read(sincedb_path).split(" ")
        position_field_index = 3
        # tailing, no delimiter, we are expecting one, if it grows we read from the start.
        # there is an info log telling us that no lines were seen but we can't test for it.
        expect(sincedb_record_fields[position_field_index]).to eq("11")
      end
    end

    describe "reading fixtures" do
      let(:directory) { FIXTURE_DIR }

      context "for an uncompressed file" do
        let(:watch_dir) { ::File.join(directory, "unc*.log") }
        let(:file_path) { ::File.join(directory, 'uncompressed.log') }

        it "the file is read" do
          FileWatch.make_fixture_current(file_path)
          actions.activate
          reading.watch_this(watch_dir)
          reading.subscribe(observer)
          expect(observer.listener_for(file_path).calls).to eq([:open, :accept, :accept, :eof, :delete])
          expect(observer.listener_for(file_path).lines.size).to eq(2)
        end
      end

      context "for another uncompressed file" do
        let(:watch_dir) { ::File.join(directory, "invalid*.log") }
        let(:file_path) { ::File.join(directory, 'invalid_utf8.gbk.log') }

        it "the file is read" do
          FileWatch.make_fixture_current(file_path)
          actions.activate
          reading.watch_this(watch_dir)
          reading.subscribe(observer)
          expect(observer.listener_for(file_path).calls).to eq([:open, :accept, :accept, :eof, :delete])
          expect(observer.listener_for(file_path).lines.size).to eq(2)
        end
      end

      context "for a compressed file" do
        let(:watch_dir) { ::File.join(directory, "compressed.*.gz") }
        let(:file_path) { ::File.join(directory, 'compressed.log.gz') }

        it "the file is read" do
          FileWatch.make_fixture_current(file_path)
          actions.activate
          reading.watch_this(watch_dir)
          reading.subscribe(observer)
          expect(observer.listener_for(file_path).calls).to eq([:open, :accept, :accept, :eof, :delete])
          expect(observer.listener_for(file_path).lines.size).to eq(2)
        end
      end
    end
  end
end
