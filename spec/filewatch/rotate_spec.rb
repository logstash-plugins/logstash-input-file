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
  describe Watch, :unix => true do
    let(:directory) { Pathname.new(Stud::Temporary.directory) }
    let(:file1_path) { file_path.to_path }
    let(:max)   { 4095 }
    let(:stat_interval) { 0.01 }
    let(:discover_interval) { 15 }
    let(:start_new_files_at) { :end }
    let(:sincedb_path) { directory.join("tailing.sdb") }
    let(:opts) do
      {
        :stat_interval => stat_interval, :start_new_files_at => start_new_files_at, :max_open_files => max,
        :delimiter => "\n", :discover_interval => discover_interval, :sincedb_path => sincedb_path.to_path
      }
    end
    let(:observer) { TestObserver.new }
    let(:tailing) { ObservingTail.new(opts) }
    let(:line1) { "Line 1 - Lorem ipsum dolor sit amet, consectetur adipiscing elit." }
    let(:line2) { "Line 2 - Proin ut orci lobortis, congue diam in, dictum est." }
    let(:line3) { "Line 3 - Sed vestibulum accumsan sollicitudin." }

    before do
      directory
      wait(1.0).for{Dir.exist?(directory)}.to eq(true)
    end

    after do
      FileUtils.rm_rf(directory)
      wait(1.0).for{Dir.exist?(directory)}.to eq(false)
    end

    context "create + rename rotation: when a new logfile is renamed to a path we have seen before and the open file is fully read, renamed outside glob" do
      let(:watch_dir) { directory.join("*A.log") }
      let(:file_path) { directory.join("1A.log") }
      subject { described_class.new(conf) }
      let(:listener1) { observer.listener_for(file1_path) }
      let(:listener2) { observer.listener_for(second_file.to_path) }
      let(:actions) do
        RSpec::Sequencing
          .run_after(0.25, "create file") do
            file_path.open("wb") { |file|  file.write("#{line1}\n") }
          end
          .then_after(0.25, "write a 'unfinished' line") do
            file_path.open("ab") { |file|  file.write(line2) }
          end
          .then_after(0.25, "rotate once") do
            tmpfile = directory.join("1.logtmp")
            tmpfile.open("wb") { |file|  file.write("\n#{line3}\n")}
            file_path.rename(directory.join("1.log.1"))
            FileUtils.mv(directory.join("1.logtmp").to_path, file1_path)
          end
          .then("wait for expectation") do
            sleep(0.25) # if ENV['CI']
            wait(2).for { listener1.calls }.to eq([:open, :accept, :accept, :accept])
          end
          .then("quit") do
            tailing.quit
          end
      end

      it "content from both inodes are sent via the same stream" do
        actions.activate_quietly
        tailing.watch_this(watch_dir.to_path)
        tailing.subscribe(observer)
        actions.assert_no_errors
        lines = listener1.lines
        expect(lines[0]).to eq(line1)
        expect(lines[1]).to eq(line2)
        expect(lines[2]).to eq(line3)
      end
    end

    context "create + rename rotation: a multiple file rename cascade" do
      let(:watch_dir) { directory.join("*B.log") }
      let(:file_path) { directory.join("1B.log") }
      subject { described_class.new(conf) }
      let(:second_file) { directory.join("2B.log") }
      let(:third_file) { directory.join("3B.log") }
      let(:listener1) { observer.listener_for(file1_path) }
      let(:listener2) { observer.listener_for(second_file.to_path) }
      let(:listener3) { observer.listener_for(third_file.to_path) }
      let(:actions) do
        RSpec::Sequencing
          .run_after(0.25, "create file") do
            file_path.open("wb") { |file|  file.write("#{line1}\n") }
          end
          .then_after(0.25, "rotate 1 - line1(66) is in 2B.log, line2(61) is in 1B.log") do
            file_path.rename(second_file)
            file_path.open("wb") { |file|  file.write("#{line2}\n") }
          end
          .then_after(0.25, "rotate 2 - line1(66) is in 3B.log, line2(61) is in 2B.log, line3(47) is in 1B.log") do
            second_file.rename(third_file)
            file_path.rename(second_file)
            file_path.open("wb") { |file|  file.write("#{line3}\n") }
          end
          .then("wait for expectations to be met") do
            wait(0.75).for{listener1.lines.size == 3 && listener3.lines.empty? && listener2.lines.empty?}.to eq(true)
          end
          .then("quit") do
            tailing.quit
          end
      end

      it "content from both inodes are sent via the same stream" do
        actions.activate_quietly
        tailing.watch_this(watch_dir.to_path)
        tailing.subscribe(observer)
        actions.assert_no_errors
        expect(listener1.lines[0]).to eq(line1)
        expect(listener1.lines[1]).to eq(line2)
        expect(listener1.lines[2]).to eq(line3)
      end
    end

    context "create + rename rotation: a two file rename cascade in slow motion" do
      let(:watch_dir) { directory.join("*C.log") }
      let(:file_path) { directory.join("1C.log") }
      let(:stat_interval) { 0.01 }
      subject { described_class.new(conf) }
      let(:second_file) { directory.join("2C.log") }
      let(:listener1) { observer.listener_for(file1_path) }
      let(:listener2) { observer.listener_for(second_file.to_path) }
      let(:actions) do
        RSpec::Sequencing
          .run_after(0.25, "create original - write line 1, 66 bytes") do
            file_path.open("wb") { |file|  file.write("#{line1}\n") }
          end
          .then_after(0.25, "rename to 2.log") do
            file_path.rename(second_file)
          end
          .then_after(0.25, "write line 2 to original, 61 bytes") do
            file_path.open("wb") { |file|  file.write("#{line2}\n") }
          end
          .then_after(0.25, "rename to 2.log again") do
            file_path.rename(second_file)
          end
          .then_after(0.25, "write line 3 to original, 47 bytes") do
            file_path.open("wb") { |file|  file.write("#{line3}\n") }
          end
          .then("wait for expectations to be met") do
            wait(1).for{listener1.lines.size == 3 && listener2.lines.empty?}.to eq(true)
          end
          .then("quit") do
            tailing.quit
          end
      end

      it "content from both inodes are sent via the same stream AND content from the rotated file is not read again" do
        actions.activate_quietly
        tailing.watch_this(watch_dir.to_path)
        tailing.subscribe(observer)
        actions.assert_no_errors
        expect(listener1.lines[0]).to eq(line1)
        expect(listener1.lines[1]).to eq(line2)
        expect(listener1.lines[2]).to eq(line3)
      end
    end

    context "create + rename rotation: a two file rename cascade in normal speed" do
      let(:watch_dir) { directory.join("*D.log") }
      let(:file_path) { directory.join("1D.log") }
      subject { described_class.new(conf) }
      let(:second_file) { directory.join("2D.log") }
      let(:listener1) { observer.listener_for(file1_path) }
      let(:listener2) { observer.listener_for(second_file.to_path) }
      let(:actions) do
        RSpec::Sequencing
          .run_after(0.25, "create original - write line 1, 66 bytes") do
            file_path.open("wb") { |file|  file.write("#{line1}\n") }
          end
          .then_after(0.25, "rename to 2.log") do
            file_path.rename(second_file)
            file_path.open("wb") { |file|  file.write("#{line2}\n") }
          end
          .then_after(0.25, "rename to 2.log again") do
            file_path.rename(second_file)
            file_path.open("wb") { |file|  file.write("#{line3}\n") }
          end
          .then("wait for expectations to be met") do
            wait(0.5).for{listener1.lines.size == 3 && listener2.lines.empty?}.to eq(true)
          end
          .then("quit") do
            tailing.quit
          end
      end

      it "content from both inodes are sent via the same stream AND content from the rotated file is not read again" do
        actions.activate_quietly
        tailing.watch_this(watch_dir.to_path)
        tailing.subscribe(observer)
        actions.assert_no_errors
        expect(listener1.lines[0]).to eq(line1)
        expect(listener1.lines[1]).to eq(line2)
        expect(listener1.lines[2]).to eq(line3)
      end
    end

    context "create + rename rotation: when a new logfile is renamed to a path we have seen before but not all content from the previous the file is read" do
      let(:opts) { super().merge(
          :file_chunk_size => line1.bytesize.succ,
          :file_chunk_count => 1
        ) }
      let(:watch_dir) { directory.join("*E.log") }
      let(:file_path) { directory.join("1E.log") }
      subject { described_class.new(conf) }
      let(:listener1) { observer.listener_for(file1_path) }
      let(:actions) do
        RSpec::Sequencing
          .run_after(0.25, "create file") do
            file_path.open("wb") do |file|
              65.times{file.puts(line1)}
            end
          end
          .then_after(0.25, "rotate") do
            tmpfile = directory.join("1E.logtmp")
            tmpfile.open("wb") { |file|  file.puts(line1)}
            file_path.rename(directory.join("1E.log.1"))
            tmpfile.rename(directory.join("1E.log"))
          end
          .then("wait for expectations to be met") do
            wait(0.5).for{listener1.lines.size}.to eq(66)
          end
          .then("quit") do
            tailing.quit
          end
      end

      it "content from both inodes are sent via the same stream" do
        actions.activate_quietly
        tailing.watch_this(watch_dir.to_path)
        tailing.subscribe(observer)
        actions.assert_no_errors
        expected_calls = ([:accept] * 66).unshift(:open)
        expect(listener1.lines.uniq).to eq([line1])
        expect(listener1.calls).to eq(expected_calls)
        expect(sincedb_path.readlines.size).to eq(2)
      end
    end

    context "copy + truncate rotation: when a logfile is copied to a new path and truncated and the open file is fully read" do
      let(:watch_dir) { directory.join("*F.log") }
      let(:file_path) { directory.join("1F.log") }
      subject { described_class.new(conf) }
      let(:listener1) { observer.listener_for(file1_path) }
      let(:actions) do
        RSpec::Sequencing
          .run_after(0.25, "create file") do
            file_path.open("wb") { |file|  file.puts(line1); file.puts(line2) }
          end
          .then_after(0.25, "rotate") do
            FileUtils.cp(file1_path, directory.join("1F.log.1").to_path)
            file_path.truncate(0)
          end
          .then_after(0.25, "write to truncated file") do
            file_path.open("wb") { |file|  file.puts(line3) }
          end
          .then("wait for expectations to be met") do
            wait(0.5).for{listener1.lines.size}.to eq(3)
          end
          .then("quit") do
            tailing.quit
          end
      end

      it "content is read correctly" do
        actions.activate_quietly
        tailing.watch_this(watch_dir.to_path)
        tailing.subscribe(observer)
        actions.assert_no_errors
        expect(listener1.lines).to eq([line1, line2, line3])
        expect(listener1.calls).to eq([:open, :accept, :accept, :accept])
      end
    end

    context "copy + truncate rotation: when a logfile is copied to a new path and truncated before the open file is fully read" do
      let(:opts) { super().merge(
          :file_chunk_size => line1.bytesize.succ,
          :file_chunk_count => 1
        ) }
      let(:watch_dir) { directory.join("*G.log") }
      let(:file_path) { directory.join("1G.log") }
      subject { described_class.new(conf) }
      let(:listener1) { observer.listener_for(file1_path) }
      let(:actions) do
        RSpec::Sequencing
          .run_after(0.25, "create file") do
            file_path.open("wb") { |file|  65.times{file.puts(line1)} }
          end
          .then_after(0.25, "rotate") do
            FileUtils.cp(file1_path, directory.join("1G.log.1").to_path)
            file_path.truncate(0)
          end
          .then_after(0.25, "write to truncated file") do
            file_path.open("wb") { |file|  file.puts(line3) }
          end
          .then("wait for expectations to be met") do
            wait(0.5).for{listener1.lines.last}.to eq(line3)
          end
          .then("quit") do
            tailing.quit
          end
      end

      it "unread content before the truncate is lost" do
        actions.activate_quietly
        tailing.watch_this(watch_dir.to_path)
        tailing.subscribe(observer)
        actions.assert_no_errors
        expect(listener1.lines.size).to be < 66
      end
    end

    context "? rotation: when an active file is renamed inside the glob and the reading does not lag" do
      let(:watch_dir) { directory.join("*H.log") }
      let(:file_path) { directory.join("1H.log") }
      let(:file2) { directory.join("2H.log") }
      subject { described_class.new(conf) }
      let(:listener1) { observer.listener_for(file1_path) }
      let(:listener2) { observer.listener_for(file2.to_path) }
      let(:actions) do
        RSpec::Sequencing
          .run_after(0.25, "create file") do
            file_path.open("wb") { |file|  file.puts(line1); file.puts(line2) }
          end
          .then_after(0.25, "rename") do
            FileUtils.mv(file1_path, file2.to_path)
          end
          .then_after(0.25, "write to renamed file") do
            file2.open("ab") { |file|  file.puts(line3) }
          end
          .then("wait for expectations to be met") do
            wait(0.75).for{listener1.lines.size + listener2.lines.size}.to eq(3)
          end
          .then("quit") do
            tailing.quit
          end
      end

      it "content is read correctly, the renamed file is not reread from scratch" do
        actions.activate_quietly
        tailing.watch_this(watch_dir.to_path)
        tailing.subscribe(observer)
        actions.assert_no_errors
        expect(listener1.lines).to eq([line1, line2])
        expect(listener2.lines).to eq([line3])
      end
    end

    context "? rotation: when an active file is renamed inside the glob and the reading lags behind" do
      let(:opts) { super().merge(
          :file_chunk_size => line1.bytesize.succ,
          :file_chunk_count => 2
        ) }
      let(:watch_dir) { directory.join("*I.log") }
      let(:file_path) { directory.join("1I.log") }
      let(:file2) { directory.join("2I.log") }
      subject { described_class.new(conf) }
      let(:listener1) { observer.listener_for(file1_path) }
      let(:listener2) { observer.listener_for(file2.to_path) }
      let(:actions) do
        RSpec::Sequencing
          .run_after(0.25, "create file") do
            file_path.open("wb") { |file| 65.times{file.puts(line1)} }
          end
          .then_after(0.25, "rename") do
            FileUtils.mv(file1_path, file2.to_path)
          end
          .then_after(0.25, "write to renamed file") do
            file2.open("ab") { |file|  file.puts(line3) }
          end
          .then("wait for expectations to be met") do
            wait(1.25).for{listener1.lines.size + listener2.lines.size}.to eq(66)
          end
          .then("quit") do
            tailing.quit
          end
      end

      it "content is read correctly, the renamed file is not reread from scratch" do
        actions.activate_quietly
        tailing.watch_this(watch_dir.to_path)
        tailing.subscribe(observer)
        actions.assert_no_errors
        expect(listener2.lines.last).to eq(line3)
      end
    end

    context "? rotation: when a not active file is rotated outside the glob before the file is read" do
      let(:opts) { super().merge(
          :close_older => 3600,
          :max_open_files => 1,
          :file_sort_by => "path"
        ) }
      let(:watch_dir) { directory.join("*J.log") }
      let(:file_path) { directory.join("1J.log") }
      let(:file2) { directory.join("2J.log") }
      let(:file3) { directory.join("2J.log.1") }
      let(:listener1) { observer.listener_for(file1_path) }
      let(:listener2) { observer.listener_for(file2.to_path) }
      let(:listener3) { observer.listener_for(file3.to_path) }
      subject { described_class.new(conf) }
      let(:actions) do
        RSpec::Sequencing
          .run_after(0.25, "create file") do
            file_path.open("wb") { |file| 65.times{file.puts(line1)} }
            file2.open("wb")     { |file| 65.times{file.puts(line1)} }
          end
          .then_after(0.25, "rename") do
            FileUtils.mv(file2.to_path, file3.to_path)
          end
          .then("wait for expectations to be met") do
            wait(1.25).for{listener1.lines.size}.to eq(65)
          end
          .then("quit") do
            tailing.quit
          end
      end

      it "file 1 content is read correctly, the renamed file 2 is not read at all" do
        actions.activate_quietly
        tailing.watch_this(watch_dir.to_path)
        tailing.subscribe(observer)
        actions.assert_no_errors
        expect(listener2.lines.size).to eq(0)
        expect(listener3.lines.size).to eq(0)
      end
    end

    context "? rotation: when an active file is renamed inside the glob - issue 214" do
      let(:watch_dir) { directory.join("*L.log") }
      let(:file_path) { directory.join("1L.log") }
      let(:second_file) { directory.join("2L.log") }
      subject { described_class.new(conf) }
      let(:listener1) { observer.listener_for(file1_path) }
      let(:listener2) { observer.listener_for(second_file.to_path) }
      let(:stat_interval) { 0.25 }
      let(:discover_interval) { 1 }
      let(:line4) { "Line 4 - Some other non lorem ipsum content" }
      let(:actions) do
        RSpec::Sequencing
        .run_after(0.75, "create file") do
          file_path.open("wb") { |file|  file.puts(line1); file.puts(line2) }
        end
        .then_after(0.5, "rename") do
          file_path.rename(second_file)
          file_path.open("wb") { |file|  file.puts("#{line3}") }
        end
        .then("wait for expectations to be met") do
          wait(2.0).for{listener1.lines.size + listener2.lines.size}.to eq(3)
        end
        .then_after(0.5, "rename again") do
          file_path.rename(second_file)
          file_path.open("wb") { |file|  file.puts("#{line4}") }
        end
        .then("wait for expectations to be met") do
          wait(2.0).for{listener1.lines.size + listener2.lines.size}.to eq(4)
        end
        .then("quit") do
          tailing.quit
        end
      end

      it "content is read correctly, the renamed file is not reread from scratch" do
        actions.activate_quietly
        tailing.watch_this(watch_dir.to_path)
        tailing.subscribe(observer)
        actions.assert_no_errors
        expect(listener1.lines).to eq([line1, line2, line3, line4])
        expect(listener2.lines).to eq([])
      end
    end
  end
end
