# encoding: utf-8
require 'stud/temporary'
require_relative 'spec_helper'
require 'filewatch/observing_tail'

module FileWatch
  describe Watch do
    let(:directory) { Stud::Temporary.directory }
    let(:watch_dir)  { ::File.join(directory, "*#{suffix}.log") }
    let(:file_path)  { ::File.join(directory, "1#{suffix}.log") }
    let(:file_path2) { ::File.join(directory, "2#{suffix}.log") }
    let(:file_path3) { ::File.join(directory, "3#{suffix}.log") }
    let(:max) { 4095 }
    let(:stat_interval) { 0.1 }
    let(:discover_interval) { 4 }
    let(:start_new_files_at) { :end }
    let(:sincedb_path) { ::File.join(directory, "tailing.sdb") }
    let(:opts) do
      {
        :stat_interval => stat_interval,
        :start_new_files_at => start_new_files_at,
        :max_open_files => max,
        :delimiter => "\n",
        :discover_interval => discover_interval,
        :sincedb_path => sincedb_path,
        :file_sort_by => "path"
      }
    end
    let(:observer) { TestObserver.new }
    let(:listener1) { observer.listener_for(file_path) }
    let(:listener2) { observer.listener_for(file_path2) }
    let(:listener3) { observer.listener_for(file_path3) }
    let(:tailing) { ObservingTail.new(opts) }

    before do
      directory
      wait(1.0).for { Dir.exist?(directory) }.to eq(true)
    end

    after do
      FileUtils.rm_rf(directory)
    end

    describe "max open files (set to 1)" do
      let(:max) { 1 }
      let(:wait_before_quit) { 0.15 }
      let(:stat_interval) { 0.01 }
      let(:discover_interval) { 4 }
      let(:start_new_files_at) { :beginning }
      let(:actions) do
        RSpec::Sequencing
          .run_after(wait_before_quit, "quit after a short time") do
            tailing.quit
          end
      end

      before do
        ENV["FILEWATCH_MAX_FILES_WARN_INTERVAL"] = "0"
        File.open(file_path, "wb")  { |file| file.write("line1\nline2\n") }
        File.open(file_path2, "wb") { |file| file.write("line-A\nline-B\n") }
      end

      context "when max_active is 1" do
        let(:suffix) { "A" }
        it "without close_older set, opens only 1 file" do
          actions.activate_quietly
          # create files before first discovery, they will be read from the end
          tailing.watch_this(watch_dir)
          tailing.subscribe(observer)
          actions.assert_no_errors
          expect(tailing.settings.max_active).to eq(max)
          expect(listener1.lines).to eq(["line1", "line2"])
          expect(listener1.calls).to eq([:open, :accept, :accept])
          expect(listener2.calls).to be_empty
        end
      end

      context "when close_older is set" do
        let(:wait_before_quit) { 0.8 }
        let(:opts) { super().merge(:close_older => 0.1, :max_open_files => 1, :stat_interval => 0.1) }
        let(:suffix) { "B" }
        it "opens both files" do
          actions.activate_quietly
          tailing.watch_this(watch_dir)
          tailing.subscribe(observer)
          actions.assert_no_errors
          expect(tailing.settings.max_active).to eq(1)
          expect(listener2.calls).to eq([:open, :accept, :accept, :timed_out])
          expect(listener2.lines).to eq(["line-A", "line-B"])
          expect(listener1.calls).to eq([:open, :accept, :accept, :timed_out])
          expect(listener1.lines).to eq(["line1", "line2"])
        end
      end
    end

    context "when watching a directory with files, existing content is skipped" do
      let(:suffix) { "C" }
      let(:actions) do
        RSpec::Sequencing
          .run("create file") do
            File.open(file_path, "wb") { |file| file.write("lineA\nlineB\n") }
          end
          .then_after(0.1, "begin watching") do
            tailing.watch_this(watch_dir)
          end
          .then_after(1.0, "add content") do
            File.open(file_path, "ab") { |file| file.write("line1\nline2\n") }
          end
          .then("wait") do
            wait(0.75).for { listener1.lines }.to_not be_empty
          end
          .then("quit") do
            tailing.quit
          end
      end

      it "only the new content is read" do
        actions.activate_quietly
        tailing.subscribe(observer)
        actions.assert_no_errors
        expect(listener1.calls).to eq([:open, :accept, :accept])
        expect(listener1.lines).to eq(["line1", "line2"])
      end
    end

    context "when watching a directory without files and one is added" do
      let(:suffix) { "D" }
      let(:actions) do
        RSpec::Sequencing
          .run("begin watching") do
            tailing.watch_this(watch_dir)
          end
          .then_after(0.1, "create file") do
            File.open(file_path, "wb") { |file|  file.write("line1\nline2\n") }
          end
          .then("wait") do
            wait(0.75).for { listener1.lines }.to_not be_empty
          end
          .then("quit") do
            tailing.quit
          end
      end

      it "the file is read from the beginning" do
        actions.activate_quietly
        tailing.subscribe(observer)
        actions.assert_no_errors
        expect(listener1.calls).to eq([:open, :accept, :accept])
        expect(listener1.lines).to eq(["line1", "line2"])
      end
    end

    context "given a previously discovered file" do
      # these tests rely on the fact that the 'filepath' does not exist on disk
      # it simulates that the user deleted the file
      # so when a stat is taken on the file an error is raised
      let(:suffix) { "E" }
      let(:quit_after) { 0.2 }
      let(:stat)  { double("stat", :size => 100, :modified_at => Time.now.to_f, :inode => 234567, :inode_struct => InodeStruct.new("234567", 1, 5)) }
      let(:watched_file) { WatchedFile.new(file_path, stat, tailing.settings) }
      before do
        allow(stat).to receive(:restat).and_raise(Errno::ENOENT)
        tailing.watch.watched_files_collection.add(watched_file)
        watched_file.initial_completed
      end

      context "when a close operation occurs" do
        before { watched_file.close }
        it "is removed from the watched_files_collection" do
          expect(tailing.watch.watched_files_collection).not_to be_empty
          RSpec::Sequencing.run_after(quit_after, "quit") { tailing.quit }
          tailing.subscribe(observer)
          expect(tailing.watch.watched_files_collection).to be_empty
          expect(listener1.calls).to eq([:delete])
        end
      end

      context "an ignore operation occurs" do
        before { watched_file.ignore }
        it "is removed from the watched_files_collection" do
          RSpec::Sequencing.run_after(quit_after, "quit") { tailing.quit }
          tailing.subscribe(observer)
          expect(tailing.watch.watched_files_collection).to be_empty
          expect(listener1.calls).to eq([:delete])
        end
      end

      context "when subscribed and a watched file is no longer readable" do
        before { watched_file.watch }
        it "is removed from the watched_files_collection" do
          RSpec::Sequencing.run_after(quit_after, "quit") { tailing.quit }
          tailing.subscribe(observer)
          expect(tailing.watch.watched_files_collection).to be_empty
          expect(listener1.calls).to eq([:delete])
        end
      end

      context "when subscribed and an active file is no longer readable" do
        before { watched_file.activate }
        it "is removed from the watched_files_collection" do
          RSpec::Sequencing.run_after(quit_after, "quit") { tailing.quit }
          tailing.subscribe(observer)
          expect(tailing.watch.watched_files_collection).to be_empty
          expect(listener1.calls).to eq([:delete])
        end
      end
    end

    context "when a processed file shrinks" do
      let(:discover_interval) { 1 }
      let(:suffix) { "F" }
      let(:actions) do
        RSpec::Sequencing
        .run_after(0.1, "start watching") do
          tailing.watch_this(watch_dir)
        end
        .then_after(0.1, "create file") do
          # create file after first discovery, will be read from the start
          File.open(file_path, "wb") { |file|  file.write("line1\nline2\nline3\nline4\n") }
        end
        .then("wait for initial lines to be read") do
          wait(0.8).for{listener1.lines.size}.to eq(4), "listener1.lines.size not eq 4"
        end
        .then_after(0.25, "truncate file and write new content") do
          File.truncate(file_path, 0)
          File.open(file_path, "ab") { |file|  file.write("lineA\nlineB\n") }
          wait(0.5).for{listener1.lines.size}.to eq(6), "listener1.lines.size not eq 6"
        end
        .then("quit") do
          tailing.quit
        end
      end

      it "new changes to the shrunk file are read from the beginning" do
        actions.activate_quietly
        tailing.subscribe(observer)
        actions.assert_no_errors
        expect(listener1.calls).to eq([:open, :accept, :accept, :accept, :accept, :accept, :accept])
        expect(listener1.lines).to eq(["line1", "line2", "line3", "line4", "lineA", "lineB"])
      end
    end

    context "when watching a directory with files and a file is renamed to not match glob", :unix => true do
      let(:suffix) { "G" }
      let(:new_file_path) { file_path + ".old" }
      let(:new_file_listener) { observer.listener_for(new_file_path) }
      let(:actions) do
        RSpec::Sequencing
          .run("start watching") do
            tailing.watch_this(watch_dir)
          end
          .then_after(0.1, "create file") do
            # create file after first discovery, will be read from the beginning
            File.open(file_path, "wb") { |file|  file.write("line1\nline2\n") }
          end
          .then_after(0.55, "rename file") do
            FileUtils.mv(file_path, new_file_path)
          end
          .then_after(0.55, "then write to renamed file") do
            File.open(new_file_path, "ab") { |file|  file.write("line3\nline4\n") }
            wait(0.5).for{listener1.lines.size}.to eq(2), "listener1.lines.size not eq(2)"
          end
          .then_after(0.1, "quit") do
            tailing.quit
          end
      end

      it "changes to the renamed file are not read" do
        actions.activate_quietly
        tailing.subscribe(observer)
        actions.assert_no_errors
        expect(listener1.calls).to eq([:open, :accept, :accept, :delete])
        expect(listener1.lines).to eq(["line1", "line2"])
        expect(new_file_listener.calls).to eq([])
        expect(new_file_listener.lines).to eq([])
      end
    end

    context "when watching a directory with files and a file is renamed to match glob", :unix => true do
      let(:suffix) { "H" }
      let(:opts) { super().merge(:close_older => 0) }
      let(:listener2) { observer.listener_for(file_path2) }
      let(:actions) do
        RSpec::Sequencing
          .run("file created") do
            # create file before first discovery, will be read from the end
            File.open(file_path, "wb") { |file|  file.write("line1\nline2\n") }
          end
          .then_after(0.15, "start watching after files are written") do
            tailing.watch_this(watch_dir)
          end
          .then("wait") do
            wait(0.5).for{listener1.calls.last}.to eq(:timed_out)
          end
          .then("rename file") do
            FileUtils.mv(file_path, file_path2)
          end
          .then_after(0.1, "then write to renamed file") do
            File.open(file_path2, "ab") { |file|  file.write("line3\nline4\n") }
          end
          .then_after(0.1, "wait for lines") do
            wait(0.5).for{listener2.lines.size}.to eq(2)
          end
          .then_after(0.1, "quit") do
            tailing.quit
          end
      end

      it "the first set of lines are not re-read" do
        actions.activate_quietly
        tailing.subscribe(observer)
        actions.assert_no_errors
        expect(listener1.lines).to eq([])
        expect(listener1.calls).to eq([:open, :timed_out, :delete])
        expect(listener2.lines).to eq(["line3", "line4"])
        expect(listener2.calls).to eq([:open, :accept, :accept, :timed_out])
      end
    end

    context "when watching a directory with files and data is appended" do
      let(:suffix) { "I" }
      let(:actions) do
        RSpec::Sequencing
          .run("file created") do
            File.open(file_path, "wb") { |file|  file.write("line1\nline2\n") }
          end
          .then_after(0.15, "start watching after file is written") do
            tailing.watch_this(watch_dir)
          end
          .then_after(0.45, "append more lines to the file") do
            File.open(file_path, "ab") { |file|  file.write("line3\nline4\n") }
            wait(0.5).for{listener1.lines.size}.to eq(2)
          end
          .then_after(0.1, "quit") do
            tailing.quit
          end
      end

      it "appended lines are read only" do
        actions.activate_quietly
        tailing.subscribe(observer)
        actions.assert_no_errors
        expect(listener1.calls).to eq([:open, :accept, :accept])
        expect(listener1.lines).to eq(["line3", "line4"])
      end
    end

    context "when close older expiry is enabled" do
      let(:opts) { super().merge(:close_older => 1) }
      let(:suffix) { "J" }
      let(:actions) do
        RSpec::Sequencing.run("create file") do
          File.open(file_path, "wb") { |file|  file.write("line1\nline2\n") }
        end
        .then("watch and wait") do
          tailing.watch_this(watch_dir)
          wait(1.25).for{listener1.calls}.to eq([:open, :timed_out])
        end
        .then("quit") do
          tailing.quit
        end
      end

      it "existing lines are not read and the file times out" do
        actions.activate_quietly
        tailing.subscribe(observer)
        actions.assert_no_errors
        expect(listener1.lines).to eq([])
      end
    end

    context "when close older expiry is enabled and after timeout the file is appended-to" do
      let(:opts) { super().merge(:close_older => 0.5) }
      let(:suffix) { "K" }
      let(:actions) do
        RSpec::Sequencing
          .run("start watching") do
            tailing.watch_this(watch_dir)
          end
          .then("create file") do
            File.open(file_path, "wb") { |file|  file.write("line1\nline2\n") }
          end
          .then("wait for file to be read") do
            wait(0.5).for{listener1.calls}.to eq([:open, :accept, :accept]), "file is not read"
          end
          .then("wait for file to be read and time out") do
            wait(0.75).for{listener1.calls}.to eq([:open, :accept, :accept, :timed_out]), "file did not timeout the first time"
          end
          .then("append more lines to file after file ages more than close_older") do
            File.open(file_path, "ab") { |file|  file.write("line3\nline4\n") }
          end
          .then("wait for last timeout") do
            wait(0.75).for{listener1.calls}.to eq([:open, :accept, :accept, :timed_out, :open, :accept, :accept, :timed_out]), "file did not timeout the second time"
          end
          .then("quit") do
            tailing.quit
          end
      end

      it "all lines are read" do
        actions.activate_quietly
        tailing.subscribe(observer)
        actions.assert_no_errors
        expect(listener1.lines).to eq(["line1", "line2", "line3", "line4"])
      end
    end

    context "when ignore older expiry is enabled and all files are already expired" do
      let(:opts) { super().merge(:ignore_older => 1) }
      let(:suffix) { "L" }
      let(:actions) do
        RSpec::Sequencing
          .run("create file older than ignore_older and watch") do
            File.open(file_path, "wb") { |file|  file.write("line1\nline2\n") }
            FileWatch.make_file_older(file_path, 15)
            tailing.watch_this(watch_dir)
          end
          .then_after(1.1, "quit") do
            tailing.quit
          end
      end

      it "no files are read" do
        actions.activate_quietly
        tailing.subscribe(observer)
        expect(listener1.calls).to eq([])
        expect(listener1.lines).to eq([])
      end
    end

    context "when a file is renamed before it gets activated", :unix => true do
      let(:max) { 1 }
      let(:opts) { super().merge(:file_chunk_count => 8, :file_chunk_size => 6, :close_older => 0.1, :discover_interval => 6) }
      let(:suffix) { "M" }
      let(:start_new_files_at) { :beginning } # we are creating files and sincedb record before hand
      let(:actions) do
        RSpec::Sequencing
          .run("create files and sincedb record") do
            File.open(file_path, "wb") { |file| 32.times{file.write("line1\n")} }
            File.open(file_path2, "wb") { |file| file.write("line2\n") }
            # synthesize a sincedb record
            stat = File.stat(file_path2)
            record = [stat.ino.to_s, stat.dev_major.to_s, stat.dev_minor.to_s, "0", "1526220348.083179", file_path2]
            File.open(sincedb_path, "wb") { |file| file.puts(record.join(" ")) }
          end
          .then_after(0.2, "watch") do
            tailing.watch_this(watch_dir)
          end
          .then_after(0.1, "rename file 2") do
            FileUtils.mv(file_path2, file_path3)
          end
          .then("wait") do
            wait(4).for do
              listener1.lines.size == 32 && listener2.calls == [:delete] && listener3.calls == [:open, :accept, :timed_out]
            end.to eq(true), "listener1.lines != 32 or listener2.calls != [:delete] or listener3.calls != [:open, :accept, :timed_out]"
          end
          .then("quit") do
            tailing.quit
          end
      end

      it "files are read correctly" do
        actions.activate_quietly
        tailing.subscribe(observer)
        actions.assert_no_errors
        expect(listener2.lines).to eq([])
        expect(listener3.lines).to eq(["line2"])
      end
    end

    context "when ignore_older is less than close_older and all files are not expired" do
      let(:opts) { super().merge(:ignore_older => 1, :close_older => 1.1) }
      let(:suffix) { "N" }
      let(:start_new_files_at) { :beginning }
      let(:actions) do
        RSpec::Sequencing
          .run_after(0.1, "file created") do
            File.open(file_path, "wb") { |file|  file.write("line1\nline2\n") }
          end
          .then("start watching before file age reaches ignore_older") do
            tailing.watch_this(watch_dir)
          end
          .then("wait for lines") do
            wait(1.5).for{listener1.calls}.to eq([:open, :accept, :accept, :timed_out])
          end
          .then("quit") do
            tailing.quit
          end
      end

      it "reads lines normally" do
        actions.activate_quietly
        tailing.subscribe(observer)
        actions.assert_no_errors
        expect(listener1.lines).to eq(["line1", "line2"])
      end
    end

    context "when ignore_older is less than close_older and all files are expired" do
      let(:opts) { super().merge(:ignore_older => 10, :close_older => 1) }
      let(:suffix) { "P" }
      let(:actions) do
        RSpec::Sequencing
          .run("creating file") do
            File.open(file_path, "wb") { |file|  file.write("line1\nline2\n") }
          end
          .then("making it older by 15 seconds and watch") do
            FileWatch.make_file_older(file_path, 15)
            tailing.watch_this(watch_dir)
          end
          .then_after(0.75, "quit after allowing time to check the files") do
            tailing.quit
          end
      end

      it "no files are read" do
        actions.activate_quietly
        tailing.subscribe(observer)
        expect(listener1.calls).to eq([])
        expect(listener1.lines).to eq([])
      end
    end

    context "when ignore older and close older expiry is enabled and after timeout the file is appended-to" do
      let(:opts) { super().merge(:ignore_older => 20, :close_older => 0.5) }
      let(:suffix) { "Q" }
      let(:actions) do
        RSpec::Sequencing
          .run("file older than ignore_older created and watching") do
            File.open(file_path, "wb") { |file|  file.write("line1\nline2\n") }
            FileWatch.make_file_older(file_path, 25)
            tailing.watch_this(watch_dir)
          end
          .then_after(0.15, "append more lines to file after file ages more than ignore_older") do
            File.open(file_path, "ab") { |file|  file.write("line3\nline4\n") }
          end
          .then("wait for lines") do
            wait(2).for{listener1.calls}.to eq([:open, :accept, :accept, :timed_out])
          end
          .then_after(0.1, "quit after allowing time to close the file") do
            tailing.quit
          end
      end

      it "reads the added lines only" do
        actions.activate_quietly
        tailing.subscribe(observer)
        actions.assert_no_errors
        expect(listener1.lines).to eq(["line3", "line4"])
      end
    end

    context "when a non default delimiter is specified and it is not in the content" do
      let(:opts) { super().merge(:ignore_older => 20, :close_older => 1, :delimiter => "\nø") }
      let(:suffix) { "R" }
      let(:actions) do
        RSpec::Sequencing
          .run("start watching") do
            tailing.watch_this(watch_dir)
          end
          .then("creating file") do
            File.open(file_path, "wb") { |file|  file.write("line1\nline2") }
          end
          .then("wait for :timeout") do
            wait(2).for{listener1.calls}.to eq([:open, :timed_out])
          end
          .then_after(0.75, "quit after allowing time to close the file") do
            tailing.quit
          end
      end

      it "the file is opened, data is read, but no lines are found, the file times out" do
        actions.activate_quietly
        tailing.subscribe(observer)
        actions.assert_no_errors
        expect(listener1.lines).to eq([])
        sincedb_record_fields = File.read(sincedb_path).split(" ")
        position_field_index = 3
        # tailing, no delimiter, we are expecting one, if it grows we read from the start.
        # there is an info log telling us that no lines were seen but we can't test for it.
        expect(sincedb_record_fields[position_field_index]).to eq("0")
      end
    end
  end
end
