# encoding: utf-8

require "helpers/spec_helper"
require "logstash/inputs/file"

# LogStash::Logging::Logger::configure_logging("DEBUG")

require "tempfile"
require "stud/temporary"
require "logstash/codecs/multiline"

FILE_DELIMITER = LogStash::Environment.windows? ? "\r\n" : "\n"

describe LogStash::Inputs::File do
  describe "'read' mode testing with input(conf) do |pipeline, queue|" do
    it "should start at the beginning of an existing file and delete the file when done" do
      tmpfile_path = Stud::Temporary.pathname
      sincedb_path = Stud::Temporary.pathname

      conf = <<-CONFIG
        input {
          file {
            type => "blah"
            path => "#{tmpfile_path}"
            sincedb_path => "#{sincedb_path}"
            delimiter => "#{FILE_DELIMITER}"
            mode => "read"
            file_completed_action => "delete"
          }
        }
      CONFIG

      File.open(tmpfile_path, "a") do |fd|
        fd.puts("hello")
        fd.puts("world")
        fd.fsync
      end

      events = input(conf) do |pipeline, queue|
        2.times.collect { queue.pop }
      end

      expect(events.map{|e| e.get("message")}).to contain_exactly("hello", "world")
      expect(File.exist?(tmpfile_path)).to be_falsey
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
            delimiter => "#{FILE_DELIMITER}"
            mode => "read"
            file_completed_action => "log"
            file_completed_log_path => "#{log_completed_path}"
          }
        }
        CONFIG

        events = input(conf) do |pipeline, queue|
          2.times.collect { queue.pop }
        end

        expect(events[0].get("message")).to start_with("2010-03-12   23:51")
        expect(events[1].get("message")).to start_with("2010-03-12   23:51")
        expect(IO.read(log_completed_path)).to eq(file_path.to_s + "\n")
      end

    end

    context "for an uncompressed file" do
      let(:file_path) { fixture_dir.join('uncompressed.log') }

      it "the file is read and the path is logged to the `file_completed_log_path` file" do
        tmpfile_path = fixture_dir.join("unc*.log")
        sincedb_path = Stud::Temporary.pathname
        FileInput.make_fixture_current(file_path.to_path)
        log_completed_path = Stud::Temporary.pathname

        conf = <<-CONFIG
        input {
          file {
            type => "blah"
            path => "#{tmpfile_path}"
            sincedb_path => "#{sincedb_path}"
            delimiter => "#{FILE_DELIMITER}"
            mode => "read"
            file_completed_action => "log"
            file_completed_log_path => "#{log_completed_path}"
          }
        }
        CONFIG

        events = input(conf) do |pipeline, queue|
          2.times.collect { queue.pop }
        end

        expect(events[0].get("message")).to start_with("2010-03-12   23:51")
        expect(events[1].get("message")).to start_with("2010-03-12   23:51")
        expect(IO.read(log_completed_path)).to eq(file_path.to_s + "\n")
      end
    end

    context "for a compressed file" do
      it "the file is read" do
        tmpfile_path = fixture_dir.join("compressed.*.*")
        sincedb_path = Stud::Temporary.pathname
        file_path = fixture_dir.join('compressed.log.gz')
        file_path2 = fixture_dir.join('compressed.log.gzip')
        FileInput.make_fixture_current(file_path.to_path)
        log_completed_path = Stud::Temporary.pathname

        conf = <<-CONFIG
        input {
          file {
            type => "blah"
            path => "#{tmpfile_path}"
            sincedb_path => "#{sincedb_path}"
            delimiter => "#{FILE_DELIMITER}"
            mode => "read"
            file_completed_action => "log"
            file_completed_log_path => "#{log_completed_path}"
          }
        }
        CONFIG

        events = input(conf) do |pipeline, queue|
          4.times.collect { queue.pop }
        end

        expect(events[0].get("message")).to start_with("2010-03-12   23:51")
        expect(events[1].get("message")).to start_with("2010-03-12   23:51")
        expect(events[2].get("message")).to start_with("2010-03-12   23:51")
        expect(events[3].get("message")).to start_with("2010-03-12   23:51")
        logged_completions = IO.read(log_completed_path).split
        expect(logged_completions.first).to match(/compressed\.log\.(gzip|gz)$/)
        expect(logged_completions.last).to match(/compressed\.log\.(gzip|gz)$/)
      end
    end
  end
end
