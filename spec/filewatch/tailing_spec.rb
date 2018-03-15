
require 'stud/temporary'
require_relative 'spec_helper'
require 'filewatch/observing_tail'

LogStash::Logging::Logger::configure_logging("WARN")
# LogStash::Logging::Logger::configure_logging("DEBUG")

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
    let(:results)   { [] }
    let(:stat_interval) { 0.1 }
    let(:discover_interval) { 4 }
    let(:start_new_files_at) { :beginning }
    let(:sincedb_path) { ::File.join(directory, "tailing.sdb") }
    let(:opts) do
      {
        :stat_interval => stat_interval, :start_new_files_at => start_new_files_at,
        :delimiter => "\n", :discover_interval => discover_interval, :sincedb_path => sincedb_path
      }
    end
    let(:observer) { TestObserver.new }
    let(:tailing) { ObservingTail.new(opts) }

    after do
      FileUtils.rm_rf(directory)
    end

    describe "max open files (set to 1)" do
      let(:max) { 1 }
      let(:file_path2) { File.join(directory, "2.log") }
      let(:wait_before_quit) { 0.15 }
      let(:stat_interval) { 0.01 }
      let(:discover_interval) { 4 }
      let(:actions) do
        RSpec::Sequencing
          .run_after(wait_before_quit, "quit after a short time") do
            tailing.quit
          end
      end

      before do
        ENV["FILEWATCH_MAX_OPEN_FILES"] = max.to_s
        ENV["FILEWATCH_MAX_FILES_WARN_INTERVAL"] = "0"
        File.open(file_path, "wb")  { |file| file.write("line1\nline2\n") }
        File.open(file_path2, "wb") { |file| file.write("lineA\nlineB\n") }
      end

      after do
        ENV.delete("FILEWATCH_MAX_OPEN_FILES")
        ENV.delete("FILEWATCH_MAX_FILES_WARN_INTERVAL")
      end

      context "when using ENV" do
        it "without close_older set, opens only 1 file" do
          actions.activate
          tailing.watch_this(watch_dir)
          tailing.subscribe(observer)
          expect(OPTS.max_active).to eq(max)
          file1_calls = observer.listener_for(file_path).calls
          file2_calls = observer.listener_for(file_path2).calls
          # file glob order is OS dependent
          if file1_calls.empty?
            expect(observer.listener_for(file_path2).lines).to eq(["lineA", "lineB"])
            expect(file2_calls).to eq([:open, :accept, :accept])
          else
            expect(observer.listener_for(file_path).lines).to eq(["line1", "line2"])
            expect(file1_calls).to eq([:open, :accept, :accept])
            expect(file2_calls).to be_empty
          end
        end
      end

      context "when close_older is set" do
        let(:wait_before_quit) { 0.4 }
        let(:opts) { super.merge(:close_older => 0.2, :max_active => 1, :stat_interval => 0.1) }
        it "opens both files" do
          actions.activate
          tailing.watch_this(watch_dir)
          tailing.subscribe(observer)
          expect(OPTS.max_active).to eq(1)
          filelistener_1 = observer.listener_for(file_path)
          filelistener_2 = observer.listener_for(file_path2)
          expect(filelistener_2.calls).to eq([:open, :accept, :accept, :timed_out])
          expect(filelistener_2.lines).to eq(["lineA", "lineB"])
          expect(filelistener_1.calls).to eq([:open, :accept, :accept])
          expect(filelistener_1.lines).to eq(["line1", "line2"])
        end
      end
    end

    context "when watching a directory with files" do
      let(:start_new_files_at) { :beginning }
      let(:actions) do
        RSpec::Sequencing.run_after(0.45, "quit after a short time") do
          tailing.quit
        end
      end

      it "the file is read" do
        File.open(file_path, "wb") { |file|  file.write("line1\nline2\n") }
        actions.activate
        tailing.watch_this(watch_dir)
        tailing.subscribe(observer)
        expect(observer.listener_for(file_path).calls).to eq([:open, :accept, :accept])
        expect(observer.listener_for(file_path).lines).to eq(["line1", "line2"])
      end
    end

    context "when watching a directory without files and one is added" do
      let(:start_new_files_at) { :beginning }
      before do
        tailing.watch_this(watch_dir)
        RSpec::Sequencing
          .run_after(0.25, "create file") do
            File.open(file_path, "wb") { |file|  file.write("line1\nline2\n") }
          end
          .then_after(0.45, "quit after a short time") do
            tailing.quit
          end
      end

      it "the file is read" do
        tailing.subscribe(observer)
        expect(observer.listener_for(file_path).calls).to eq([:open, :accept, :accept])
        expect(observer.listener_for(file_path).lines).to eq(["line1", "line2"])
      end
    end

    describe "given a previously discovered file" do
      # these tests rely on the fact that the 'filepath' does not exist on disk
      # it simulates that the user deleted the file
      # so when a stat is taken on the file an error is raised
      let(:quit_after) { 0.1 }
      let(:stat)  { double("stat", :size => 100, :ctime => Time.now, :mtime => Time.now, :ino => 234567, :dev_major => 3, :dev_minor => 2) }
      let(:watched_file) { WatchedFile.new_initial(file_path, stat) }

      before do
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
          expect(observer.listener_for(file_path).calls).to eq([:delete])
        end
      end

      context "an ignore operation occurs" do
        before { watched_file.ignore }
        it "is removed from the watched_files_collection" do
          RSpec::Sequencing.run_after(quit_after, "quit") { tailing.quit }
          tailing.subscribe(observer)
          expect(tailing.watch.watched_files_collection).to be_empty
          expect(observer.listener_for(file_path).calls).to eq([:delete])
        end
      end

      context "when subscribed and a watched file is no longer readable" do
        before { watched_file.watch }
        it "is removed from the watched_files_collection" do
          RSpec::Sequencing.run_after(quit_after, "quit") { tailing.quit }
          tailing.subscribe(observer)
          expect(tailing.watch.watched_files_collection).to be_empty
          expect(observer.listener_for(file_path).calls).to eq([:delete])
        end
      end

      context "when subscribed and an active file is no longer readable" do
        before { watched_file.activate }
        it "is removed from the watched_files_collection" do
          RSpec::Sequencing.run_after(quit_after, "quit") { tailing.quit }
          tailing.subscribe(observer)
          expect(tailing.watch.watched_files_collection).to be_empty
          expect(observer.listener_for(file_path).calls).to eq([:delete])
        end
      end
    end

    context "when a processed file shrinks" do
      let(:discover_interval) { 100 }
      before do
        RSpec::Sequencing
        .run("create file") do
          File.open(file_path, "wb") { |file|  file.write("line1\nline2\nline3\nline4\n") }
        end
        .then_after(0.25, "start watching after files are written") do
          tailing.watch_this(watch_dir)
        end
        .then_after(0.25, "truncate file and write new content") do
          File.truncate(file_path, 0)
          File.open(file_path, "wb") { |file|  file.write("lineA\nlineB\n") }
        end
        .then_after(0.25, "quit after a short time") do
          tailing.quit
        end
      end

      it "new changes to the shrunk file are read from the beginning" do
        tailing.subscribe(observer)
        expect(observer.listener_for(file_path).calls).to eq([:open, :accept, :accept, :accept, :accept, :accept, :accept])
        expect(observer.listener_for(file_path).lines).to eq(["line1", "line2", "line3", "line4", "lineA", "lineB"])
      end
    end

    context "when watching a directory with files and a file is renamed to not match glob" do
      let(:new_file_path) { file_path + ".old" }
      before do
        RSpec::Sequencing
          .run("create file") do
            File.open(file_path, "wb") { |file|  file.write("line1\nline2\n") }
          end
          .then_after(0.25, "start watching after files are written") do
            tailing.watch_this(watch_dir)
          end
          .then_after(0.55, "rename file") do
            FileUtils.mv(file_path, new_file_path)
          end
          .then_after(0.55, "then write to renamed file") do
            File.open(new_file_path, "ab") { |file|  file.write("line3\nline4\n") }
          end
          .then_after(0.45, "quit after a short time") do
            tailing.quit
          end
      end

      it "changes to the renamed file are not read" do
        tailing.subscribe(observer)
        expect(observer.listener_for(file_path).calls).to eq([:open, :accept, :accept, :delete])
        expect(observer.listener_for(file_path).lines).to eq(["line1", "line2"])
        expect(observer.listener_for(new_file_path).calls).to eq([])
        expect(observer.listener_for(new_file_path).lines).to eq([])
      end
    end

    context "when watching a directory with files and a file is renamed to match glob" do
      let(:new_file_path) { file_path + "2.log" }
      let(:opts) { super.merge(:close_older => 0) }
      before do
        RSpec::Sequencing
          .run("create file") do
            File.open(file_path, "wb") { |file|  file.write("line1\nline2\n") }
          end
          .then_after(0.15, "start watching after files are written") do
            tailing.watch_this(watch_dir)
          end
          .then_after(0.25, "rename file") do
            FileUtils.mv(file_path, new_file_path)
          end
          .then("then write to renamed file") do
            File.open(new_file_path, "ab") { |file|  file.write("line3\nline4\n") }
          end
          .then_after(0.55, "quit after a short time") do
            tailing.quit
          end
      end

      it "the first set of lines are not re-read" do
        tailing.subscribe(observer)
        expect(observer.listener_for(file_path).calls).to eq([:open, :accept, :accept, :timed_out, :delete])
        expect(observer.listener_for(file_path).lines).to eq(["line1", "line2"])
        expect(observer.listener_for(new_file_path).calls).to eq([:open, :accept, :accept, :timed_out])
        expect(observer.listener_for(new_file_path).lines).to eq(["line3", "line4"])
      end
    end

    context "when watching a directory with files and data is appended" do
      before do
        RSpec::Sequencing
          .run("create file") do
            File.open(file_path, "wb") { |file|  file.write("line1\nline2\n") }
          end
          .then_after(0.25, "start watching after file is written") do
            tailing.watch_this(watch_dir)
          end
          .then_after(0.45, "append more lines to the file") do
            File.open(file_path, "ab") { |file|  file.write("line3\nline4\n") }
          end
          .then_after(0.45, "quit after a short time") do
            tailing.quit
          end
      end

      it "appended lines are read after an EOF" do
        tailing.subscribe(observer)
        expect(observer.listener_for(file_path).calls).to eq([:open, :accept, :accept, :accept, :accept])
        expect(observer.listener_for(file_path).lines).to eq(["line1", "line2", "line3", "line4"])
      end
    end

    context "when close older expiry is enabled" do
      let(:opts) { super.merge(:close_older => 1) }
      before do
        RSpec::Sequencing
          .run("create file") do
            File.open(file_path, "wb") { |file|  file.write("line1\nline2\n") }
          end
          .then("start watching before file ages more than close_older") do
            tailing.watch_this(watch_dir)
          end
          .then_after(2.1, "quit after allowing time to close the file") do
            tailing.quit
          end
      end

      it "lines are read and the file times out" do
        tailing.subscribe(observer)
        expect(observer.listener_for(file_path).calls).to eq([:open, :accept, :accept, :timed_out])
        expect(observer.listener_for(file_path).lines).to eq(["line1", "line2"])
      end
    end

    context "when close older expiry is enabled and after timeout the file is appended-to" do
      let(:opts) { super.merge(:close_older => 1) }
      before do
        RSpec::Sequencing
          .run("create file") do
            File.open(file_path, "wb") { |file|  file.write("line1\nline2\n") }
          end
          .then("start watching before file ages more than close_older") do
            tailing.watch_this(watch_dir)
          end
          .then_after(2.1, "append more lines to file after file ages more than close_older") do
            File.open(file_path, "ab") { |file|  file.write("line3\nline4\n") }
          end
          .then_after(2.1, "quit after allowing time to close the file") do
            tailing.quit
          end
      end

      it "all lines are read" do
        tailing.subscribe(observer)
        expect(observer.listener_for(file_path).calls).to eq([:open, :accept, :accept, :timed_out, :open, :accept, :accept, :timed_out])
        expect(observer.listener_for(file_path).lines).to eq(["line1", "line2", "line3", "line4"])
      end
    end

    context "when ignore older expiry is enabled and all files are already expired" do
      let(:opts) { super.merge(:ignore_older => 1) }
      before do
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
        tailing.subscribe(observer)
        expect(observer.listener_for(file_path).calls).to eq([])
        expect(observer.listener_for(file_path).lines).to eq([])
      end
    end

    context "when ignore_older is less than close_older and all files are not expired" do
      let(:opts) { super.merge(:ignore_older => 1, :close_older => 1.5) }
      before do
        RSpec::Sequencing
          .run("create file") do
            File.open(file_path, "wb") { |file|  file.write("line1\nline2\n") }
          end
          .then("start watching before file age reaches ignore_older") do
            tailing.watch_this(watch_dir)
          end
          .then_after(1.6, "quit after allowing time to close the file") do
            tailing.quit
          end
      end

      it "reads lines normally" do
        tailing.subscribe(observer)
        expect(observer.listener_for(file_path).calls).to eq([:open, :accept, :accept])
        expect(observer.listener_for(file_path).lines).to eq(["line1", "line2"])
      end
    end

    context "when ignore_older is less than close_older and all files are expired" do
      let(:opts) { super.merge(:ignore_older => 10, :close_older => 1) }
      before do
        RSpec::Sequencing
          .run("create file older than ignore_older and watch") do
            File.open(file_path, "wb") { |file|  file.write("line1\nline2\n") }
            FileWatch.make_file_older(file_path, 15)
            tailing.watch_this(watch_dir)
          end
          .then_after(1.5, "quit after allowing time to check the files") do
            tailing.quit
          end
      end

      it "no files are read" do
        tailing.subscribe(observer)
        expect(observer.listener_for(file_path).calls).to eq([])
        expect(observer.listener_for(file_path).lines).to eq([])
      end
    end

    context "when ignore older and close older expiry is enabled and after timeout the file is appended-to" do
      let(:opts) { super.merge(:ignore_older => 20, :close_older => 1) }
      before do
        RSpec::Sequencing
          .run("create file older than ignore_older and watch") do
            File.open(file_path, "wb") { |file|  file.write("line1\nline2\n") }
            FileWatch.make_file_older(file_path, 25)
            tailing.watch_this(watch_dir)
          end
          .then_after(0.15, "append more lines to file after file ages more than ignore_older") do
            File.open(file_path, "ab") { |file|  file.write("line3\nline4\n") }
          end
          .then_after(1.25, "quit after allowing time to close the file") do
            tailing.quit
          end
      end

      it "reads the added lines only" do
        tailing.subscribe(observer)
        expect(observer.listener_for(file_path).lines).to eq(["line3", "line4"])
        expect(observer.listener_for(file_path).calls).to eq([:open, :accept, :accept, :timed_out])
      end
    end
  end
end


