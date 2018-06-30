# encoding: utf-8

require "helpers/spec_helper"
require "logstash/inputs/file"

# LogStash::Logging::Logger::configure_logging("DEBUG")

require "tempfile"
require "stud/temporary"
require "logstash/codecs/multiline"

describe LogStash::Inputs::File do
  describe "'read' mode testing with input(conf) do |pipeline, queue|" do
    it "should start at the beginning of an existing file and delete the file when done" do
      directory = Stud::Temporary.directory
      tmpfile_path = ::File.join(directory, "A.log")
      sincedb_path = ::File.join(directory, "readmode_A_sincedb.txt")
      path_path = ::File.join(directory, "*.log")

      conf = <<-CONFIG
        input {
          file {
            id => "blah"
            path => "#{path_path}"
            sincedb_path => "#{sincedb_path}"
            delimiter => "|"
            mode => "read"
            file_completed_action => "delete"
          }
        }
      CONFIG

      File.open(tmpfile_path, "a") do |fd|
        fd.write("hello|world")
        fd.fsync
      end

      events = input(conf) do |pipeline, queue|
        wait(0.5).for{File.exist?(tmpfile_path)}.to be_falsey
        2.times.collect { queue.pop }
      end

      expect(events.map{|e| e.get("message")}).to contain_exactly("hello", "world")
    end

    it "should start at the beginning of an existing file and log the file when done" do
      directory = Stud::Temporary.directory
      tmpfile_path = ::File.join(directory, "A.log")
      sincedb_path = ::File.join(directory, "readmode_A_sincedb.txt")
      path_path = ::File.join(directory, "*.log")
      log_completed_path = ::File.join(directory, "A_completed.txt")

      conf = <<-CONFIG
        input {
          file {
            id => "blah"
            path => "#{path_path}"
            sincedb_path => "#{sincedb_path}"
            delimiter => "|"
            mode => "read"
            file_completed_action => "log"
            file_completed_log_path => "#{log_completed_path}"
          }
        }
      CONFIG

      File.open(tmpfile_path, "a") do |fd|
        fd.write("hello|world")
        fd.fsync
      end

      events = input(conf) do |pipeline, queue|
        wait(0.5).for{IO.read(log_completed_path)}.to match(/A\.log/)
        2.times.collect { queue.pop }
      end
      expect(events.map{|e| e.get("message")}).to contain_exactly("hello", "world")
    end
  end

  describe "reading fixtures" do
    let(:fixture_dir) { Pathname.new(FileInput::FIXTURE_DIR).expand_path }

    context "for a file without a final newline character" do
      let(:file_path) { fixture_dir.join('no-final-newline.log') }

      it "the file is read and the path is logged to the `file_completed_log_path` file" do
        tmpfile_path = fixture_dir.join("no-f*.log")
        sincedb_path = Stud::Temporary.pathname
        FileInput.make_fixture_current(file_path.to_path)
        log_completed_path = Stud::Temporary.pathname

        conf = <<-CONFIG
        input {
          file {
            type => "blah"
            path => "#{tmpfile_path}"
            sincedb_path => "#{sincedb_path}"
            mode => "read"
            file_completed_action => "log"
            file_completed_log_path => "#{log_completed_path}"
          }
        }
        CONFIG

        events = input(conf) do |pipeline, queue|
          wait(0.5).for{IO.read(log_completed_path)}.to match(/#{file_path.to_s}/)
          2.times.collect { queue.pop }
        end

        expect(events[0].get("message")).to start_with("2010-03-12   23:51")
        expect(events[1].get("message")).to start_with("2010-03-12   23:51")
      end

    end

    context "for an uncompressed file" do
      let(:file_path) { fixture_dir.join('uncompressed.log') }

      it "the file is read and the path is logged to the `file_completed_log_path` file" do
        FileInput.make_fixture_current(file_path.to_path)
        tmpfile_path = fixture_dir.join("unc*.log")
        directory = Stud::Temporary.directory
        sincedb_path = ::File.join(directory, "readmode_B_sincedb.txt")
        log_completed_path = ::File.join(directory, "B_completed.txt")

        conf = <<-CONFIG
        input {
          file {
            type => "blah"
            path => "#{tmpfile_path}"
            sincedb_path => "#{sincedb_path}"
            mode => "read"
            file_completed_action => "log"
            file_completed_log_path => "#{log_completed_path}"
          }
        }
        CONFIG

        events = input(conf) do |pipeline, queue|
          wait(0.5).for{IO.read(log_completed_path)}.to match(/uncompressed\.log/)
          2.times.collect { queue.pop }
        end

        expect(events[0].get("message")).to start_with("2010-03-12   23:51")
        expect(events[1].get("message")).to start_with("2010-03-12   23:51")
      end
    end

    context "for a compressed file" do
      it "the file is read" do
        file_path = fixture_dir.join('compressed.log.gz')
        file_path2 = fixture_dir.join('compressed.log.gzip')
        FileInput.make_fixture_current(file_path.to_path)
        FileInput.make_fixture_current(file_path2.to_path)
        tmpfile_path = fixture_dir.join("compressed.*.*")
        directory = Stud::Temporary.directory
        sincedb_path = ::File.join(directory, "readmode_C_sincedb.txt")
        log_completed_path = ::File.join(directory, "C_completed.txt")

        conf = <<-CONFIG
        input {
          file {
            type => "blah"
            path => "#{tmpfile_path}"
            sincedb_path => "#{sincedb_path}"
            mode => "read"
            file_completed_action => "log"
            file_completed_log_path => "#{log_completed_path}"
          }
        }
        CONFIG

        events = input(conf) do |pipeline, queue|
          wait(0.5).for{IO.read(log_completed_path).scan(/compressed\.log\.gz(ip)?/).size}.to eq(2)
          4.times.collect { queue.pop }
        end

        expect(events[0].get("message")).to start_with("2010-03-12   23:51")
        expect(events[1].get("message")).to start_with("2010-03-12   23:51")
        expect(events[2].get("message")).to start_with("2010-03-12   23:51")
        expect(events[3].get("message")).to start_with("2010-03-12   23:51")
      end
    end
  end
end
