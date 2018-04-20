# encoding: utf-8

require "helpers/spec_helper"
require "logstash/inputs/file"

require "tempfile"
require "stud/temporary"
require "logstash/codecs/multiline"

# LogStash::Logging::Logger::configure_logging("DEBUG")

TEST_FILE_DELIMITER = LogStash::Environment.windows? ? "\r\n" : "\n"

describe LogStash::Inputs::File do
  describe "'tail' mode testing with input(conf) do |pipeline, queue|" do
    it_behaves_like "an interruptible input plugin" do
      let(:config) do
        {
          "path" => Stud::Temporary.pathname,
          "sincedb_path" => Stud::Temporary.pathname
        }
      end
    end

    it "should start at the beginning of an existing file" do
      tmpfile_path = Stud::Temporary.pathname
      sincedb_path = Stud::Temporary.pathname

      conf = <<-CONFIG
        input {
          file {
            type => "blah"
            path => "#{tmpfile_path}"
            start_position => "beginning"
            sincedb_path => "#{sincedb_path}"
            delimiter => "#{TEST_FILE_DELIMITER}"
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
    end

    it "should restart at the sincedb value" do
      tmpfile_path = Stud::Temporary.pathname
      sincedb_path = Stud::Temporary.pathname

      conf = <<-CONFIG
        input {
          file {
            type => "blah"
            path => "#{tmpfile_path}"
            start_position => "beginning"
            sincedb_path => "#{sincedb_path}"
            delimiter => "#{TEST_FILE_DELIMITER}"
          }
        }
      CONFIG

      File.open(tmpfile_path, "w") do |fd|
        fd.puts("hello3")
        fd.puts("world3")
      end

      events = input(conf) do |pipeline, queue|
        2.times.collect { queue.pop }
      end

      expect(events.map{|e| e.get("message")}).to contain_exactly("hello3", "world3")

      File.open(tmpfile_path, "a") do |fd|
        fd.puts("foo")
        fd.puts("bar")
        fd.puts("baz")
        fd.fsync
      end

      events = input(conf) do |pipeline, queue|
        3.times.collect { queue.pop }
      end
      messages = events.map{|e| e.get("message")}
      expect(messages).to contain_exactly("foo", "bar", "baz")
    end

    it "should not overwrite existing path and host fields" do
      tmpfile_path = Stud::Temporary.pathname
      sincedb_path = Stud::Temporary.pathname

      conf = <<-CONFIG
        input {
          file {
            type => "blah"
            path => "#{tmpfile_path}"
            start_position => "beginning"
            sincedb_path => "#{sincedb_path}"
            delimiter => "#{TEST_FILE_DELIMITER}"
            codec => "json"
          }
        }
      CONFIG

      File.open(tmpfile_path, "w") do |fd|
        fd.puts('{"path": "my_path", "host": "my_host"}')
        fd.puts('{"my_field": "my_val"}')
        fd.fsync
      end

      events = input(conf) do |pipeline, queue|
        2.times.collect { queue.pop }
      end

      existing_path_index, added_path_index  = "my_val" == events[0].get("my_field") ? [1,0] : [0,1]

      expect(events[existing_path_index].get("path")).to eq "my_path"
      expect(events[existing_path_index].get("host")).to eq "my_host"
      expect(events[existing_path_index].get("[@metadata][host]")).to eq "#{Socket.gethostname.force_encoding(Encoding::UTF_8)}"

      expect(events[added_path_index].get("path")).to eq "#{tmpfile_path}"
      expect(events[added_path_index].get("host")).to eq "#{Socket.gethostname.force_encoding(Encoding::UTF_8)}"
      expect(events[added_path_index].get("[@metadata][host]")).to eq "#{Socket.gethostname.force_encoding(Encoding::UTF_8)}"
    end

    it "should read old files" do
      tmpfile_path = Stud::Temporary.pathname

      conf = <<-CONFIG
        input {
          file {
            type => "blah"
            path => "#{tmpfile_path}"
            start_position => "beginning"
            codec => "json"
          }
        }
      CONFIG

      File.open(tmpfile_path, "w") do |fd|
        fd.puts('{"path": "my_path", "host": "my_host"}')
        fd.puts('{"my_field": "my_val"}')
        fd.fsync
      end
      # arbitrary old file (2 days)
      FileInput.make_file_older(tmpfile_path, 48 * 60 * 60)

      events = input(conf) do |pipeline, queue|
        2.times.collect { queue.pop }
      end
      existing_path_index, added_path_index  = "my_val" == events[0].get("my_field") ? [1,0] : [0,1]
      expect(events[existing_path_index].get("path")).to eq "my_path"
      expect(events[existing_path_index].get("host")).to eq "my_host"
      expect(events[existing_path_index].get("[@metadata][host]")).to eq "#{Socket.gethostname.force_encoding(Encoding::UTF_8)}"

      expect(events[added_path_index].get("path")).to eq "#{tmpfile_path}"
      expect(events[added_path_index].get("host")).to eq "#{Socket.gethostname.force_encoding(Encoding::UTF_8)}"
      expect(events[added_path_index].get("[@metadata][host]")).to eq "#{Socket.gethostname.force_encoding(Encoding::UTF_8)}"
    end

    context "when sincedb_path is an existing directory" do
      let(:tmpfile_path) { Stud::Temporary.pathname }
      let(:sincedb_path) { Stud::Temporary.directory }
      subject { LogStash::Inputs::File.new("path" => tmpfile_path, "sincedb_path" => sincedb_path) }

      after :each do
        FileUtils.rm_rf(sincedb_path)
      end

      it "should raise exception" do
        expect { subject.register }.to raise_error(ArgumentError)
      end
    end
  end

  describe "testing with new, register, run and stop" do
    let(:conf)         { Hash.new }
    let(:mlconf)       { Hash.new }
    let(:events)       { Array.new }
    let(:mlcodec)      { LogStash::Codecs::Multiline.new(mlconf) }
    let(:codec)        { FileInput::CodecTracer.new }
    let(:tmpfile_path) { Stud::Temporary.pathname }
    let(:sincedb_path) { Stud::Temporary.pathname }
    let(:tmpdir_path)  { Stud::Temporary.directory }

    after :each do
      FileUtils.rm_rf(sincedb_path)
    end

    context "when data exists and then more data is appended" do
      subject { described_class.new(conf) }

      before do
        File.open(tmpfile_path, "w") do |fd|
          fd.puts("ignore me 1")
          fd.puts("ignore me 2")
          fd.fsync
        end
        mlconf.update("pattern" => "^\s", "what" => "previous")
        conf.update("type" => "blah",
              "path" => tmpfile_path,
              "sincedb_path" => sincedb_path,
              "stat_interval" => 0.1,
              "codec" => mlcodec,
              "delimiter" => TEST_FILE_DELIMITER)
      end

      it "reads the appended data only" do
        subject.register
        RSpec::Sequencing
          .run_after(0.2, "assert zero events then append two lines") do
            expect(events.size).to eq(0)
            File.open(tmpfile_path, "a") { |fd| fd.puts("hello"); fd.puts("world") }
          end
          .then_after(0.4, "quit") do
            subject.stop
          end

        subject.run(events)

        event1 = events[0]
        expect(event1).not_to be_nil
        expect(event1.get("path")).to eq tmpfile_path
        expect(event1.get("[@metadata][path]")).to eq tmpfile_path
        expect(event1.get("message")).to eq "hello"

        event2 = events[1]
        expect(event2).not_to be_nil
        expect(event2.get("path")).to eq tmpfile_path
        expect(event2.get("[@metadata][path]")).to eq tmpfile_path
        expect(event2.get("message")).to eq "world"
      end
    end

    context "when close_older config is specified" do
      let(:line)         { "line1.1-of-a" }

      subject { described_class.new(conf) }

      before do
        conf.update(
              "type" => "blah",
              "path" => "#{tmpdir_path}/*.log",
              "sincedb_path" => sincedb_path,
              "stat_interval" => 0.02,
              "codec" => codec,
              "close_older" => 0.5,
              "delimiter" => TEST_FILE_DELIMITER)

        subject.register
      end

      it "having timed_out, the identity is evicted" do
        RSpec::Sequencing
          .run("create file") do
            File.open("#{tmpdir_path}/a.log", "wb") { |file|  file.puts(line) }
          end
          .then_after(0.3, "identity is mapped") do
            expect(codec.trace_for(:accept)).to eq([true])
            expect(subject.codec.identity_count).to eq(1)
          end
          .then_after(0.3, "test for auto_flush") do
            expect(codec.trace_for(:auto_flush)).to eq([true])
            expect(subject.codec.identity_count).to eq(0)
          end
          .then_after(0.1, "quit") do
            subject.stop
          end
        subject.run(events)
      end
    end

    context "when ignore_older config is specified" do
      let(:line) { "line1.1-of-a" }
      let(:tmp_dir_file) { "#{tmpdir_path}/a.log" }

      subject { described_class.new(conf) }

      before do
        File.open(tmp_dir_file, "a") do |fd|
          fd.puts(line)
          fd.fsync
        end
        FileInput.make_file_older(tmp_dir_file, 2)
        conf.update(
              "type" => "blah",
              "path" => "#{tmpdir_path}/*.log",
              "sincedb_path" => sincedb_path,
              "stat_interval" => 0.02,
              "codec" => codec,
              "ignore_older" => 1,
              "delimiter" => TEST_FILE_DELIMITER)

        subject.register
        Thread.new { subject.run(events) }
      end

      it "the file is not read" do
        sleep 0.1
        subject.stop
        expect(codec).to receive_call_and_args(:accept, false)
        expect(codec).to receive_call_and_args(:auto_flush, false)
        expect(subject.codec.identity_count).to eq(0)
      end
    end

    context "when wildcard path and a multiline codec is specified" do
      subject { described_class.new(conf) }

      before do
        mlconf.update("pattern" => "^\s", "what" => "previous")
        conf.update(
              "type" => "blah",
              "path" => "#{tmpdir_path}/*.log",
              "sincedb_path" => sincedb_path,
              "stat_interval" => 0.05,
              "codec" => mlcodec,
              "delimiter" => TEST_FILE_DELIMITER)

        subject.register
      end

      it "collects separate multiple line events from each file" do
        actions = RSpec::Sequencing
          .run_after(0.1, "create files") do
            File.open("#{tmpdir_path}/A.log", "wb") do |fd|
              fd.puts("line1.1-of-a")
              fd.puts("  line1.2-of-a")
              fd.puts("  line1.3-of-a")
            end
            File.open("#{tmpdir_path}/z.log", "wb") do |fd|
              fd.puts("line1.1-of-z")
              fd.puts("  line1.2-of-z")
              fd.puts("  line1.3-of-z")
            end
          end
          .then_after(0.2, "assert both files are mapped as identities and stop") do
            expect(subject.codec.identity_count).to eq(2)
          end
          .then_after(0.1, "stop") do
            subject.stop
          end
          .then_after(0.2 , "stop flushes both events") do
            expect(events.size).to eq(2)
            e1, e2 = events
            e1_message = e1.get("message")
            e2_message = e2.get("message")

            # can't assume File A will be read first
            if e1_message.start_with?('line1.1-of-z')
              expect(e1.get("path")).to match(/z.log/)
              expect(e2.get("path")).to match(/A.log/)
              expect(e1_message).to eq("line1.1-of-z#{TEST_FILE_DELIMITER}  line1.2-of-z#{TEST_FILE_DELIMITER}  line1.3-of-z")
              expect(e2_message).to eq("line1.1-of-a#{TEST_FILE_DELIMITER}  line1.2-of-a#{TEST_FILE_DELIMITER}  line1.3-of-a")
            else
              expect(e1.get("path")).to match(/A.log/)
              expect(e2.get("path")).to match(/z.log/)
              expect(e1_message).to eq("line1.1-of-a#{TEST_FILE_DELIMITER}  line1.2-of-a#{TEST_FILE_DELIMITER}  line1.3-of-a")
              expect(e2_message).to eq("line1.1-of-z#{TEST_FILE_DELIMITER}  line1.2-of-z#{TEST_FILE_DELIMITER}  line1.3-of-z")
            end
          end
        subject.run(events)
        # wait for actions to complete
        actions.value
      end

      context "if auto_flush is enabled on the multiline codec" do
        let(:mlconf) { { "auto_flush_interval" => 0.5 } }

        it "an event is generated via auto_flush" do
          actions = RSpec::Sequencing
            .run_after(0.1, "create files") do
              File.open("#{tmpdir_path}/A.log", "wb") do |fd|
                fd.puts("line1.1-of-a")
                fd.puts("  line1.2-of-a")
                fd.puts("  line1.3-of-a")
              end
            end
            .then_after(0.75, "wait for auto_flush") do
              e1 = events.first
              e1_message = e1.get("message")
              expect(e1["path"]).to match(/a.log/)
              expect(e1_message).to eq("line1.1-of-a#{TEST_FILE_DELIMITER}  line1.2-of-a#{TEST_FILE_DELIMITER}  line1.3-of-a")
            end
            .then("stop") do
              subject.stop
            end
          subject.run(events)
          # wait for actions to complete
          actions.value
        end
      end
    end

    context "when #run is called multiple times", :unix => true do
      let(:file_path)    { "#{tmpdir_path}/a.log" }
      let(:buffer)       { [] }
      let(:run_thread_proc) do
        lambda { Thread.new { subject.run(buffer) } }
      end
      let(:lsof_proc) do
        lambda { `lsof -p #{Process.pid} | grep #{file_path}` }
      end

      subject { described_class.new(conf) }

      before do
        conf.update(
          "path" => tmpdir_path + "/*.log",
          "start_position" => "beginning",
          "stat_interval" => 0.1,
          "sincedb_path" => sincedb_path)

        File.open(file_path, "w") do |fd|
          fd.puts('foo')
          fd.puts('bar')
          fd.fsync
        end
      end

      it "should only actually open files when content changes are detected" do
        subject.register
        expect(lsof_proc.call).to eq("")
        # first run processes the file and records sincedb progress
        run_thread_proc.call
        wait(1).for{lsof_proc.call.scan(file_path).size}.to eq(1)
        # second run quits the first run
        # sees the file has not changed size and does not open it
        run_thread_proc.call
        wait(1).for{lsof_proc.call.scan(file_path).size}.to eq(0)
        # truncate and write less than before
        File.open(file_path, "w"){ |fd| fd.puts('baz'); fd.fsync }
        # sees the file has changed size and does open it
        wait(1).for{lsof_proc.call.scan(file_path).size}.to eq(1)
      end
    end

    describe "specifying max_open_files" do
      subject { described_class.new(conf) }
      before do
        File.open("#{tmpdir_path}/a.log", "w") do |fd|
          fd.puts("line1-of-a")
          fd.puts("line2-of-a")
          fd.fsync
        end
        File.open("#{tmpdir_path}/z.log", "w") do |fd|
          fd.puts("line1-of-z")
          fd.puts("line2-of-z")
          fd.fsync
        end
      end

      context "when close_older is NOT specified" do
        before do
          conf.clear
          conf.update(
                "type" => "blah",
                "path" => "#{tmpdir_path}/*.log",
                "sincedb_path" => sincedb_path,
                "stat_interval" => 0.1,
                "max_open_files" => 1,
                "start_position" => "beginning",
                "delimiter" => TEST_FILE_DELIMITER)
          subject.register
        end
        it "collects line events from only one file" do
          actions = RSpec::Sequencing
            .run_after(0.2, "assert one identity is mapped") do
              expect(subject.codec.identity_count).to eq(1)
            end
            .then_after(0.1, "stop") do
              subject.stop
            end
            .then_after(0.1, "stop flushes last event") do
              expect(events.size).to eq(2)
              e1, e2 = events
              if Dir.glob("#{tmpdir_path}/*.log").first =~ %r{a\.log}
                #linux and OSX have different retrieval order
                expect(e1.get("message")).to eq("line1-of-a")
                expect(e2.get("message")).to eq("line2-of-a")
              else
                expect(e1.get("message")).to eq("line1-of-z")
                expect(e2.get("message")).to eq("line2-of-z")
              end
            end
          subject.run(events)
          # wait for actions future value
          actions.value
        end
      end

      context "when close_older IS specified" do
        before do
          conf.update(
                "type" => "blah",
                "path" => "#{tmpdir_path}/*.log",
                "sincedb_path" => sincedb_path,
                "stat_interval" => 0.1,
                "max_open_files" => 1,
                "close_older" => 0.5,
                "start_position" => "beginning",
                "delimiter" => TEST_FILE_DELIMITER)
          subject.register
        end

        it "collects line events from both files" do
          actions = RSpec::Sequencing
            .run_after(0.2, "assert both identities are mapped and the first two events are built") do
              expect(subject.codec.identity_count).to eq(2)
              expect(events.size).to eq(2)
            end
            .then_after(0.8, "wait for close to flush last event of each identity") do
              expect(events.size).to eq(4)
              if Dir.glob("#{tmpdir_path}/*.log").first =~ %r{a\.log}
                #linux and OSX have different retrieval order
                e1, e2, e3, e4 = events
              else
                e3, e4, e1, e2 = events
              end
              expect(e1.get("message")).to eq("line1-of-a")
              expect(e2.get("message")).to eq("line2-of-a")
              expect(e3.get("message")).to eq("line1-of-z")
              expect(e4.get("message")).to eq("line2-of-z")
            end
            .then_after(0.1, "stop") do
              subject.stop
            end
          subject.run(events)
          # wait for actions future value
          actions.value
        end
      end
    end
  end
end
