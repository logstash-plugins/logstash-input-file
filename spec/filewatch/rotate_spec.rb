# encoding: utf-8
require 'stud/temporary'
require_relative 'spec_helper'
require 'filewatch/observing_tail'

# simulate size based rotation ala
# See https://docs.python.org/2/library/logging.handlers.html#rotatingfilehandler
# The specified file is opened and used as the stream for logging.
# If mode is not specified, 'a' is used. If encoding is not None, it is used to
# open the file with that encoding. If delay is true, then file opening is deferred
# until the first call to emit(). By default, the file grows indefinitely.
# You can use the maxBytes and backupCount values to allow the file to rollover
# at a predetermined size. When the size is about to be exceeded, the file is
# closed and a new file is silently opened for output. Rollover occurs whenever
# the current log file is nearly maxBytes in length; if either of maxBytes or
# backupCount is zero, rollover never occurs. If backupCount is non-zero, the
# system will save old log files by appending the extensions ‘.1’, ‘.2’ etc.,
# to the filename. For example, with a backupCount of 5 and a base file name of
# app.log, you would get app.log, app.log.1, app.log.2, up to app.log.5.
# The file being written to is always app.log. When this file is filled, it is
# closed and renamed to app.log.1, and if files app.log.1, app.log.2, etc.
# exist, then they are renamed to app.log.2, app.log.3 etc. respectively.

module FileWatch
  describe Watch do
    before(:all) do
      @thread_abort = Thread.abort_on_exception
      Thread.abort_on_exception = true
    end

    after(:all) do
      Thread.abort_on_exception = @thread_abort
    end

    let(:directory) { Pathname.new(Stud::Temporary.directory) }
    let(:watch_dir) { directory.join("*.log") }
    let(:file_path) { directory.join("1.log") }
    let(:full_file_path) { file_path.to_path}
    let(:max)   { 4095 }
    let(:stat_interval) { 0.01 }
    let(:discover_interval) { 5 }
    let(:start_new_files_at) { :beginning }
    let(:sincedb_path) { directory.join("tailing.sdb") }
    let(:opts) do
      {
        :stat_interval => stat_interval, :start_new_files_at => start_new_files_at, :max_active => max,
        :delimiter => "\n", :discover_interval => discover_interval, :sincedb_path => sincedb_path.to_path
      }
    end
    let(:observer) { TestObserver.new }
    let(:tailing) { ObservingTail.new(opts) }
    let(:line1) { "Lorem ipsum dolor sit amet, consectetur adipiscing elit." }
    let(:line2) { "Proin ut orci lobortis, congue diam in, dictum est." }
    let(:line3) { "Sed vestibulum accumsan sollicitudin. Nulla dapibus massa varius." }

    after do
      FileUtils.rm_rf(directory)
    end

    context "create + rename rotation: when a new logfile is renamed to a path we have seen before and the open file is fully read" do
      subject { described_class.new(conf) }
      before do
        tailing.watch_this(watch_dir.to_path)
        RSpec::Sequencing
          .run_after(0.25, "create file") do
            file_path.open("wb") { |file|  file.write("#{line1}\n") }
          end
          .then_after(0.25, "write a 'unfinished' line") do
            file_path.open("ab") { |file|  file.write(line2) }
          end
          .then_after(0.25, "rotate") do
            tmpfile = directory.join("1.logtmp")
            tmpfile.open("wb") { |file|  file.write("\n#{line3}\n")}
            file_path.rename(directory.join("1.log.1"))
            FileUtils.mv(directory.join("1.logtmp").to_path, file_path.to_path)
          end
          .then_after(0.45, "quit after a short time") do
            tailing.quit
          end
      end

      it "content from both inodes are sent via the same stream" do
        tailing.subscribe(observer)
        expect(observer.listener_for(full_file_path).lines).to eq([line1, line2, line3])
        expect(observer.listener_for(full_file_path).calls).to eq([:open, :accept, :open, :accept, :accept])
      end
    end

    context "create + rename rotation: when a new logfile is renamed to a path we have seen before but not all content from the previous the file is read" do
      let(:opts) { super.merge(
          :file_chunk_size => line1.bytesize.succ,
          :file_chunk_count => 1
        ) }
      subject { described_class.new(conf) }
      before do
        tailing.watch_this(watch_dir.to_path)
        RSpec::Sequencing
          .run_after(0.1, "create file") do
            file_path.open("wb") do |file|
              65.times{file.puts(line1)}
            end
          end
          .then_after(0.25, "rotate") do
            tmpfile = directory.join("1.logtmp")
            tmpfile.open("wb") { |file|  file.puts(line1)}
            file_path.rename(directory.join("1.log.1"))
            tmpfile.rename(directory.join("1.log"))
          end
          .then_after(0.5, "quit given enough time to finish all the reading") do
            tailing.quit
          end
      end

      it "content from both inodes are sent via the same stream" do
        tailing.subscribe(observer)
        expected_calls = ([:accept] * 65).unshift(:open).push(:open, :accept)
        expected_lines = [line1] * 66
        expect(observer.listener_for(full_file_path).lines).to eq(expected_lines)
        expect(observer.listener_for(full_file_path).calls).to eq(expected_calls)
        expect(sincedb_path.readlines.size).to eq(2)
      end
    end

    context "copy + truncate rotation: when a logfile is copied to a new path and truncated and the open file is fully read" do
      subject { described_class.new(conf) }
      before do
        tailing.watch_this(watch_dir.to_path)
        RSpec::Sequencing
          .run_after(0.25, "create file") do
            file_path.open("wb") { |file|  file.puts(line1); file.puts(line2) }
          end
          .then_after(0.5, "rotate") do
            FileUtils.cp(file_path.to_path, directory.join("1.log.1").to_path)
            file_path.truncate(0)
          end
          .then_after(0.25, "write to truncated file") do
            file_path.open("wb") { |file|  file.puts(line3) }
          end
          .then_after(0.45, "quit after a short time") do
            tailing.quit
          end
      end

      it "content is read correctly" do
        tailing.subscribe(observer)
        expect(observer.listener_for(full_file_path).lines).to eq([line1, line2, line3])
        expect(observer.listener_for(full_file_path).calls).to eq([:open, :accept, :accept, :accept])
      end
    end

    context "copy + truncate rotation: when a logfile is copied to a new path and truncated before the open file is fully read" do
      let(:opts) { super.merge(
          :file_chunk_size => line1.bytesize.succ,
          :file_chunk_count => 1
        ) }
      subject { described_class.new(conf) }
      before do
        tailing.watch_this(watch_dir.to_path)
        RSpec::Sequencing
          .run_after(0.25, "create file") do
            file_path.open("wb") { |file|  65.times{file.puts(line1)} }
          end
          .then_after(0.25, "rotate") do
            FileUtils.cp(file_path.to_path, directory.join("1.log.1").to_path)
            file_path.truncate(0)
          end
          .then_after(0.1, "write to truncated file") do
            file_path.open("wb") { |file|  file.puts(line3) }
          end
          .then_after(0.25, "quit after a short time") do
            tailing.quit
          end
      end

      it "unread content before the truncate is lost" do
        tailing.subscribe(observer)
        lines = observer.listener_for(full_file_path).lines
        expect(lines.size).to be < 66
        expect(lines.last).to eq(line3)
      end
    end

    context "? rotation: when an active file is renamed inside the glob and the reading does not lag" do
      let(:file2) { directory.join("2.log") }
      # let(:discover_interval) { 1 }
      subject { described_class.new(conf) }
      before do
        tailing.watch_this(watch_dir.to_path)
        RSpec::Sequencing
          .run_after(0.1, "create file") do
            file_path.open("wb") { |file|  file.puts(line1); file.puts(line2) }
          end
          .then_after(0.1, "rename") do
            FileUtils.mv(file_path.to_path, file2.to_path)
          end
          .then_after(0.1, "write to renamed file") do
            file2.open("ab") { |file|  file.puts(line3) }
          end
          .then_after(0.2, "quit after a short time") do
            tailing.quit
          end
      end

      it "content is read correctly, the renamed file is not reread from scratch" do
        tailing.subscribe(observer)
        expect(observer.listener_for(file_path.to_path).lines).to eq([line1, line2])
        expect(observer.listener_for(file2.to_path).lines).to eq([line3])
      end
    end

    context "? rotation: when an active file is renamed inside the glob and the reading lags behind" do
      # let(:discover_interval) { 1 }
      let(:opts) { super.merge(
          :file_chunk_size => line1.bytesize.succ,
          :file_chunk_count => 2
        ) }
      let(:file2) { directory.join("2.log") }
      subject { described_class.new(conf) }
      before do
        tailing.watch_this(watch_dir.to_path)
        RSpec::Sequencing
          .run_after(0.1, "create file") do
            file_path.open("wb") { |file| 65.times{file.puts(line1)} }
          end
          .then_after(0.1, "rename") do
            FileUtils.mv(file_path.to_path, file2.to_path)
          end
          .then_after(0.1, "write to renamed file") do
            file2.open("ab") { |file|  file.puts(line3) }
          end
          .then_after(0.75, "quit after a short time") do
            tailing.quit
          end
      end

      it "content is read correctly, the renamed file is not reread from scratch" do
        tailing.subscribe(observer)
        lines = observer.listener_for(full_file_path).lines + observer.listener_for(file2.to_path).lines
        expect(lines.size).to eq(66)
        expect(lines.last).to eq(line3)
      end
    end

    context "? rotation: when a not active file is rotated outside the glob before the file is read" do
      # let(:discover_interval) { 1 }
      let(:opts) { super.merge(
          :close_older => 3600,
          :max_active => 1
        ) }
      let(:file2) { directory.join("2.log") }
      let(:file3) { directory.join("2.log.1") }
      subject { described_class.new(conf) }
      before do
        tailing.watch_this(watch_dir.to_path)
        RSpec::Sequencing
          .run_after(0.1, "create file") do
            file_path.open("wb") { |file| 65.times{file.puts(line1)} }
            file2.open("wb")     { |file| 65.times{file.puts(line1)} }
          end
          .then_after(0.1, "rename") do
            FileUtils.mv(file2.to_path, file3.to_path)
          end
          .then_after(0.75, "quit after a short time") do
            tailing.quit
          end
      end

      it "file 1 content is read correctly, the renamed file 2 is not read at all" do
        tailing.subscribe(observer)
        lines = observer.listener_for(full_file_path).lines
        expect(lines.size).to eq(65)
        expect(observer.listener_for(file2.to_path).lines.size).to eq(0)
        expect(observer.listener_for(file3.to_path).lines.size).to eq(0)
      end
    end
  end
end
