# encoding: utf-8

require "logstash/inputs/file"
require_relative "../spec_helper"
require "tempfile"
require "stud/temporary"
require "logstash/codecs/multiline"

FILE_DELIMITER = LogStash::Environment.windows? ? "\r\n" : "\n"

describe LogStash::Inputs::File do
  describe "testing with input(conf) do |pipeline, queue|" do
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
            delimiter => "#{FILE_DELIMITER}"
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

      insist { events[0]["message"] } == "hello"
      insist { events[1]["message"] } == "world"
    end

    it "should restarts at the sincedb value" do
      tmpfile_path = Stud::Temporary.pathname
      sincedb_path = Stud::Temporary.pathname

      conf = <<-CONFIG
        input {
          file {
            type => "blah"
            path => "#{tmpfile_path}"
            start_position => "beginning"
            sincedb_path => "#{sincedb_path}"
            delimiter => "#{FILE_DELIMITER}"
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

      insist { events[0]["message"] } == "hello3"
      insist { events[1]["message"] } == "world3"

      File.open(tmpfile_path, "a") do |fd|
        fd.puts("foo")
        fd.puts("bar")
        fd.puts("baz")
        fd.fsync
      end

      events = input(conf) do |pipeline, queue|
        3.times.collect { queue.pop }
      end

      insist { events[0]["message"] } == "foo"
      insist { events[1]["message"] } == "bar"
      insist { events[2]["message"] } == "baz"
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
            delimiter => "#{FILE_DELIMITER}"
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

      insist { events[0]["path"] } == "my_path"
      insist { events[0]["host"] } == "my_host"

      insist { events[1]["path"] } == "#{tmpfile_path}"
      insist { events[1]["host"] } == "#{Socket.gethostname.force_encoding(Encoding::UTF_8)}"
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
              "delimiter" => FILE_DELIMITER)
        subject.register
        Thread.new { subject.run(events) }
      end

      it "reads the appended data only" do
        sleep 0.1
        File.open(tmpfile_path, "a") do |fd|
          fd.puts("hello")
          fd.puts("world")
          fd.fsync
        end
        # wait for one event, the last line is buffered
        expect(pause_until{ events.size == 1 }).to be_truthy
        subject.stop
        # stop flushes the second event
        expect(pause_until{ events.size == 2 }).to be_truthy

        event1 = events[0]
        expect(event1).not_to be_nil
        expect(event1["path"]).to eq tmpfile_path
        expect(event1["@metadata"]["path"]).to eq tmpfile_path
        expect(event1["message"]).to eq "hello"

        event2 = events[1]
        expect(event2).not_to be_nil
        expect(event2["path"]).to eq tmpfile_path
        expect(event2["@metadata"]["path"]).to eq tmpfile_path
        expect(event2["message"]).to eq "world"
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
              "close_older" => 1,
              "delimiter" => FILE_DELIMITER)

        subject.register
        Thread.new { subject.run(events) }
      end

      it "having timed_out, the identity is evicted" do
        sleep 0.1
        File.open("#{tmpdir_path}/a.log", "a") do |fd|
          fd.puts(line)
          fd.fsync
        end
        expect(pause_until{ subject.codec.identity_count == 1 }).to be_truthy
        expect(codec).to receive_call_and_args(:accept, [true])
        # wait for expiry to kick in and close files.
        expect(pause_until{ subject.codec.identity_count.zero? }).to be_truthy
        expect(codec).to receive_call_and_args(:auto_flush, [true])
        subject.stop
      end
    end

    context "when ignore_older config is specified" do
      let(:line) { "line1.1-of-a" }

      subject { described_class.new(conf) }

      before do
        File.open("#{tmpdir_path}/a.log", "a") do |fd|
          fd.puts(line)
          fd.fsync
        end
        sleep 1.1 # wait for file to age
        conf.update(
              "type" => "blah",
              "path" => "#{tmpdir_path}/*.log",
              "sincedb_path" => sincedb_path,
              "stat_interval" => 0.02,
              "codec" => codec,
              "ignore_older" => 1,
              "delimiter" => FILE_DELIMITER)

        subject.register
        Thread.new { subject.run(events) }
      end

      it "the file is not read" do
        sleep 0.5
        subject.stop
        expect(codec).to receive_call_and_args(:accept, false)
        expect(codec).to receive_call_and_args(:auto_flush, false)
        expect(subject.codec.identity_count).to eq(0)
      end
    end

    context "when wildcard path and a multiline codec is specified" do
      subject { described_class.new(conf) }
      let(:writer_proc) do
        -> do
          File.open("#{tmpdir_path}/a.log", "a") do |fd|
            fd.puts("line1.1-of-a")
            fd.puts("  line1.2-of-a")
            fd.puts("  line1.3-of-a")
            fd.fsync
          end
          File.open("#{tmpdir_path}/z.log", "a") do |fd|
            fd.puts("line1.1-of-z")
            fd.puts("  line1.2-of-z")
            fd.puts("  line1.3-of-z")
            fd.fsync
          end
        end
      end

      before do
        mlconf.update("pattern" => "^\s", "what" => "previous")
        conf.update(
              "type" => "blah",
              "path" => "#{tmpdir_path}/*.log",
              "sincedb_path" => sincedb_path,
              "stat_interval" => 0.05,
              "codec" => mlcodec,
              "delimiter" => FILE_DELIMITER)

        subject.register
        Thread.new { subject.run(events) }
        sleep 0.1
        writer_proc.call
      end

      it "collects separate multiple line events from each file" do
        # wait for both paths to be mapped as identities
        expect(pause_until{ subject.codec.identity_count == 2 }).to be_truthy
        subject.stop
        # stop flushes both events
        expect(pause_until{ events.size == 2 }).to be_truthy

        e1, e2 = events
        e1_message = e1["message"]
        e2_message = e2["message"]

        # can't assume File A will be read first
        if e1_message.start_with?('line1.1-of-z')
          expect(e1["path"]).to match(/z.log/)
          expect(e2["path"]).to match(/a.log/)
          expect(e1_message).to eq("line1.1-of-z#{FILE_DELIMITER}  line1.2-of-z#{FILE_DELIMITER}  line1.3-of-z")
          expect(e2_message).to eq("line1.1-of-a#{FILE_DELIMITER}  line1.2-of-a#{FILE_DELIMITER}  line1.3-of-a")
        else
          expect(e1["path"]).to match(/a.log/)
          expect(e2["path"]).to match(/z.log/)
          expect(e1_message).to eq("line1.1-of-a#{FILE_DELIMITER}  line1.2-of-a#{FILE_DELIMITER}  line1.3-of-a")
          expect(e2_message).to eq("line1.1-of-z#{FILE_DELIMITER}  line1.2-of-z#{FILE_DELIMITER}  line1.3-of-z")
        end
      end

      context "if auto_flush is enabled on the multiline codec" do
        let(:writer_proc) do
          -> do
            File.open("#{tmpdir_path}/a.log", "a") do |fd|
              fd.puts("line1.1-of-a")
              fd.puts("  line1.2-of-a")
              fd.puts("  line1.3-of-a")
            end
          end
        end
        let(:mlconf) { { "auto_flush_interval" => 1 } }

        it "an event is generated via auto_flush" do
          # wait for auto_flush
          # without it lines are buffered and pause_until would time out i.e false
          expect(pause_until{ events.size == 1 }).to be_truthy
          subject.stop

          e1 = events.first
          e1_message = e1["message"]
          expect(e1["path"]).to match(/a.log/)
          expect(e1_message).to eq("line1.1-of-a#{FILE_DELIMITER}  line1.2-of-a#{FILE_DELIMITER}  line1.3-of-a")
        end
      end
    end

    context "when #run is called multiple times", :unix => true do
      let(:file_path)    { "#{tmpdir_path}/a.log" }
      let(:buffer)       { [] }
      let(:lsof)         { [] }
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
          "sincedb_path" => sincedb_path)

        File.open(file_path, "w") do |fd|
          fd.puts('foo')
          fd.puts('bar')
          fd.fsync
        end
      end

      it "should only have one set of files open" do
        subject.register
        expect(lsof_proc.call).to eq("")
        run_thread_proc.call
        sleep 0.1
        first_lsof = lsof_proc.call
        expect(first_lsof).not_to eq("")
        run_thread_proc.call
        sleep 0.1
        second_lsof = lsof_proc.call
        expect(second_lsof).to eq(first_lsof)
      end
    end
  end
end
