# encoding: utf-8
require 'stud/temporary'
require_relative 'spec_helper'
require 'filewatch/observing_read'

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
        :stat_interval => stat_interval,
        :start_new_files_at => start_new_files_at,
        :delimiter => "\n",
        :discover_interval => discover_interval,
        :ignore_older => 3600,
        :sincedb_path => sincedb_path
      }
    end
    let(:observer) { TestObserver.new }
    let(:reading) { ObservingRead.new(opts) }
    let(:listener1) { observer.listener_for(file_path) }

    after do
      FileUtils.rm_rf(directory) unless directory =~ /fixture/
    end

    context "when watching a directory with files" do
      let(:actions) do
        RSpec::Sequencing.run("quit after a short time") do
          File.open(file_path, "wb") { |file|  file.write("line1\nline2\n") }
        end
        .then("watch") do
          reading.watch_this(watch_dir)
        end
        .then("wait") do
          wait(2).for{listener1.calls.last}.to eq(:delete)
        end
        .then("quit") do
          reading.quit
        end
      end
      it "the file is read" do
        actions.activate_quietly
        reading.subscribe(observer)
        actions.assert_no_errors
        expect(listener1.calls).to eq([:open, :accept, :accept, :eof, :delete])
        expect(listener1.lines).to eq(["line1", "line2"])
      end
    end

    context "when watching a directory with files and sincedb_path is /dev/null or NUL" do
      let(:sincedb_path) { File::NULL }
      let(:actions) do
        RSpec::Sequencing.run("quit after a short time") do
          File.open(file_path, "wb") { |file|  file.write("line1\nline2\n") }
        end
        .then("watch") do
          reading.watch_this(watch_dir)
        end
        .then("wait") do
          wait(2).for{listener1.calls.last}.to eq(:delete)
        end
        .then("quit") do
          reading.quit
        end
      end
      it "the file is read" do
        actions.activate_quietly
        reading.subscribe(observer)
        actions.assert_no_errors
        expect(listener1.calls).to eq([:open, :accept, :accept, :eof, :delete])
        expect(listener1.lines).to eq(["line1", "line2"])
      end
    end

    context "when watching a directory with files using striped reading" do
      let(:file_path2) { ::File.join(directory, "2.log") }
      # use a chunk size that does not align with the line boundaries
      let(:opts) { super().merge(:file_chunk_size => 10, :file_chunk_count => 1, :file_sort_by => "path")}
      let(:lines) { [] }
      let(:observer) { TestObserver.new(lines) }
      let(:listener2) { observer.listener_for(file_path2) }
      let(:actions) do
        RSpec::Sequencing.run("create file") do
          File.open(file_path,  "w") { |file|  file.write("string1\nstring2") }
          File.open(file_path2, "w") { |file|  file.write("stringA\nstringB") }
        end
        .then("watch") do
          reading.watch_this(watch_dir)
        end
        .then("wait") do
          wait(2).for{listener1.calls.last == :delete && listener2.calls.last == :delete}.to eq(true)
        end
        .then("quit") do
          reading.quit
        end
      end
      it "the files are read seemingly in parallel" do
        actions.activate_quietly
        reading.subscribe(observer)
        actions.assert_no_errors
        expect(listener1.calls).to eq([:open, :accept, :accept, :eof, :delete])
        expect(listener2.calls).to eq([:open, :accept, :accept, :eof, :delete])
        expect(lines).to eq(%w(string1 stringA string2 stringB))
      end
    end

    context "when a non default delimiter is specified and it is not in the content" do
      let(:opts) { super().merge(:delimiter => "\nø") }
      let(:actions) do
        RSpec::Sequencing.run("create file") do
          File.open(file_path, "wb") { |file|  file.write("line1\nline2") }
        end
        .then("watch") do
          reading.watch_this(watch_dir)
        end
        .then("wait") do
          wait(2).for{listener1.calls.last}.to eq(:delete)
        end
        .then("quit") do
          reading.quit
        end
      end
      it "the file is opened, data is read, but no lines are found initially, at EOF the whole file becomes the line" do
        actions.activate_quietly
        reading.subscribe(observer)
        actions.assert_no_errors
        expect(listener1.calls).to eq([:open, :accept, :eof, :delete])
        expect(listener1.lines).to eq(["line1\nline2"])
        sincedb_record_fields = File.read(sincedb_path).split(" ")
        position_field_index = 3
        # tailing, no delimiter, we are expecting one, if it grows we read from the start.
        # there is an info log telling us that no lines were seen but we can't test for it.
        expect(sincedb_record_fields[position_field_index]).to eq("11")
      end
    end

    context "when watching directory with files and adding a new file" do
      let(:file_path2) { ::File.join(directory, "2.log") }
      let(:file_path3) { ::File.join(directory, "3.log") }

      let(:opts) { super().merge(:file_sort_by => "last_modified") }
      let(:lines) { [] }
      let(:observer) { TestObserver.new(lines) }


      let(:listener2) { observer.listener_for(file_path2) }
      let(:listener3) { observer.listener_for(file_path3) }

      let(:actions) do
        RSpec::Sequencing.run("create12") do
          File.open(file_path,  "w") { |file| file.write("string11\nstring12") }
          File.open(file_path2, "w") { |file| file.write("string21\nstring22") }
        end
        .then("watch") do
          reading.watch_this(watch_dir)
        end
        .then("wait12") do
          wait(2).for { listener1.calls.last == :delete && listener2.calls.last == :delete }.to eq(true)
        end
        .then_after(2, "create3") do
          File.open(file_path3, "w") { |file| file.write("string31\nstring32") }
        end
        .then("wait3") do
          wait(2).for { listener3.calls.last == :delete }.to eq(true)
        end
        .then("quit") do
          reading.quit
        end
      end

      it "reads all (3) files" do
        actions.activate_quietly
        reading.subscribe(observer)
        actions.assert_no_errors
        expect(lines.last).to eq 'string32'
        expect(lines.sort).to eq %w(string11 string12 string21 string22 string31 string32)
        expect( reading.watch.watched_files_collection.paths ).to eq [ file_path, file_path2, file_path3 ]
      end
    end

    context "when watching a directory with files using exit_after_read" do
      let(:opts) { super().merge(:exit_after_read => true, :max_open_files => 2) }
      let(:file_path3) { ::File.join(directory, "3.log") }
      let(:file_path4) { ::File.join(directory, "4.log") }
      let(:file_path5) { ::File.join(directory, "5.log") }
      let(:lines) { [] }
      let(:observer) { TestObserver.new(lines) }
      let(:listener3) { observer.listener_for(file_path3) }
      let(:file_path6) { ::File.join(directory, "6.log") }
      let(:listener6) { observer.listener_for(file_path6) }

      it "the file is read" do
        File.open(file_path3, "w") { |file| file.write("line1\nline2\n") }
        reading.watch_this(watch_dir)
        reading.subscribe(observer)
        expect(listener3.lines).to eq(["line1", "line2"])
      end

      it "multiple files are read" do
        File.open(file_path3, "w") { |file| file.write("line1\nline2\n") }
        File.open(file_path4, "w") { |file| file.write("line3\nline4\n") }
        reading.watch_this(watch_dir)
        reading.subscribe(observer)
        expect(listener3.lines.sort).to eq(["line1", "line2", "line3", "line4"])
      end

      it "multiple files are read even if max_open_files is smaller then number of files" do
        File.open(file_path3, "w") { |file| file.write("line1\nline2\n") }
        File.open(file_path4, "w") { |file| file.write("line3\nline4\n") }
        File.open(file_path5, "w") { |file| file.write("line5\nline6\n") }
        reading.watch_this(watch_dir)
        reading.subscribe(observer)
        expect(listener3.lines.sort).to eq(["line1", "line2", "line3", "line4", "line5", "line6"])
      end

      it "file as marked as reading_completed" do
        File.open(file_path3, "w") { |file| file.write("line1\nline2\n") }
        reading.watch_this(watch_dir)
        reading.subscribe(observer)
        expect(listener3.calls).to eq([:open, :accept, :accept, :eof, :delete, :reading_completed])
      end

      it "sincedb works correctly" do
        File.open(file_path3, "w") { |file| file.write("line1\nline2\n") }
        reading.watch_this(watch_dir)
        reading.subscribe(observer)
        sincedb_record_fields = File.read(sincedb_path).split(" ")
        position_field_index = 3
        expect(sincedb_record_fields[position_field_index]).to eq("12")
      end

      it "does not include new files added after start" do
        File.open(file_path3, "w") { |file|  file.write("line1\nline2\n") }
        reading.watch_this(watch_dir)
        reading.subscribe(observer)
        File.open(file_path6, "w") { |file|  file.write("foob\nbar\n") }
        expect(listener3.lines).to eq(["line1", "line2"])
        expect(listener3.calls).to eq([:open, :accept, :accept, :eof, :delete, :reading_completed])
        expect(listener6.calls).to eq([])
      end
    
    end

    describe "reading fixtures" do
      let(:directory) { FIXTURE_DIR }
      let(:actions) do
        RSpec::Sequencing.run("watch") do
          reading.watch_this(watch_dir)
        end
        .then("wait") do
          wait(1).for{listener1.calls.last}.to eq(:delete)
        end
        .then("quit") do
          reading.quit
        end
      end
      context "for an uncompressed file" do
        let(:watch_dir) { ::File.join(directory, "unc*.log") }
        let(:file_path) { ::File.join(directory, 'uncompressed.log') }

        it "the file is read" do
          FileWatch.make_fixture_current(file_path)
          actions.activate_quietly
          reading.subscribe(observer)
          actions.assert_no_errors
          expect(listener1.calls).to eq([:open, :accept, :accept, :eof, :delete])
          expect(listener1.lines.size).to eq(2)
        end
      end

      context "for another uncompressed file" do
        let(:watch_dir) { ::File.join(directory, "invalid*.log") }
        let(:file_path) { ::File.join(directory, 'invalid_utf8.gbk.log') }

        it "the file is read" do
          FileWatch.make_fixture_current(file_path)
          actions.activate_quietly
          reading.subscribe(observer)
          actions.assert_no_errors
          expect(listener1.calls).to eq([:open, :accept, :accept, :eof, :delete])
          expect(listener1.lines.size).to eq(2)
        end
      end

      context "for a compressed file" do
        let(:watch_dir) { ::File.join(directory, "compressed.*.gz") }
        let(:file_path) { ::File.join(directory, 'compressed.log.gz') }

        it "the file is read" do
          FileWatch.make_fixture_current(file_path)
          actions.activate_quietly
          reading.subscribe(observer)
          actions.assert_no_errors
          expect(listener1.calls).to eq([:open, :accept, :accept, :eof, :delete])
          expect(listener1.lines.size).to eq(2)
        end
      end
    end
  end
end
