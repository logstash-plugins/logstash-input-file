# encoding: utf-8

require "logstash/devutils/rspec/spec_helper"
require "tempfile"
require "stud/temporary"
require "logstash/inputs/file"

Thread.abort_on_exception = true

describe "inputs/file" do

  delimiter = (LogStash::Environment.windows? ? "\r\n" : "\n")

  it "should starts at the end of an existing file" do
    tmpfile_path = Stud::Temporary.pathname
    sincedb_path = Stud::Temporary.pathname

    conf = <<-CONFIG
      input {
        file {
          type => "blah"
          path => "#{tmpfile_path}"
          sincedb_path => "#{sincedb_path}"
          delimiter => "#{delimiter}"
        }
      }
    CONFIG

    File.open(tmpfile_path, "w") do |fd|
      fd.puts("ignore me 1")
      fd.puts("ignore me 2")
    end

    events = input(conf) do |pipeline, queue|

      # at this point the plugins
      # threads might still be initializing so we cannot know when the
      # file plugin will have seen the original file, it could see it
      # after the first(s) hello world appends below, hence the
      # retry logic.

      events = []

      retries = 0
      while retries < 20
        File.open(tmpfile_path, "a") do |fd|
          fd.puts("hello")
          fd.puts("world")
        end

        if queue.size >= 2
          events = 2.times.collect { queue.pop }
          break
        end

        sleep(0.1)
        retries += 1
      end

      events
    end

    insist { events[0]["message"] } == "hello"
    insist { events[1]["message"] } == "world"
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
          delimiter => "#{delimiter}"
        }
      }
    CONFIG

    File.open(tmpfile_path, "a") do |fd|
      fd.puts("hello")
      fd.puts("world")
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
          delimiter => "#{delimiter}"
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
          delimiter => "#{delimiter}"
          codec => "json"
        }
      }
    CONFIG

    File.open(tmpfile_path, "w") do |fd|
      fd.puts('{"path": "my_path", "host": "my_host"}')
      fd.puts('{"my_field": "my_val"}')
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

  context "when #run is called multiple times" do
    let(:tmpdir_path)  { Stud::Temporary.directory }
    let(:sincedb_path) { Stud::Temporary.pathname }
    let(:file_path)    { "#{tmpdir_path}/a.log" }
    let(:buffer)       { [] }
    let(:lsof)         { [] }
    let(:stop_proc) do
      lambda do |input, arr|
        Thread.new(input, arr) do |i, a|
          sleep 0.5
          a << `lsof -p #{Process.pid} | grep "a.log"`
          i.stop
        end
      end
    end

    subject { LogStash::Inputs::File.new("path" => tmpdir_path + "/*.log", "start_position" => "beginning", "sincedb_path" => sincedb_path) }

    after :each do
      FileUtils.rm_rf(tmpdir_path)
      FileUtils.rm_rf(sincedb_path)
    end
    before do
      File.open(file_path, "w") do |fd|
        fd.puts('foo')
        fd.puts('bar')
      end
    end
    it "should only have one set of files open" do
      subject.register
      lsof_before = `lsof -p #{Process.pid} | grep #{file_path}`
      expect(lsof_before).to eq("")
      stop_proc.call(subject, lsof)
      subject.run(buffer)
      expect(lsof.first).not_to eq("")
      stop_proc.call(subject, lsof)
      subject.run(buffer)
      expect(lsof.last).to eq(lsof.first)
    end
  end

  context "when wildcard path and a multiline codec is specified" do
    let(:tmpdir_path)  { Stud::Temporary.directory }
    let(:sincedb_path) { Stud::Temporary.pathname }
    let(:conf) do
      <<-CONFIG
        input {
          file {
            type => "blah"
            path => "#{tmpdir_path}/*.log"
            start_position => "beginning"
            sincedb_path => "#{sincedb_path}"
            delimiter => "#{FILE_DELIMITER}"
            codec => multiline { pattern => "^\s" what => previous }
          }
        }
      CONFIG
    end

    let(:writer_proc) do
      -> do
        File.open("#{tmpdir_path}/a.log", "a") do |fd|
          fd.puts("line1.1-of-a")
          fd.puts("  line1.2-of-a")
          fd.puts("  line1.3-of-a")
          fd.puts("line2.1-of-a")
        end

        File.open("#{tmpdir_path}/z.log", "a") do |fd|
          fd.puts("line1.1-of-z")
          fd.puts("  line1.2-of-z")
          fd.puts("  line1.3-of-z")
          fd.puts("line2.1-of-z")
        end
      end
    end

    after do
      FileUtils.rm_rf(tmpdir_path)
    end

    let(:event_count) { 2 }

    it "collects separate multiple line events from each file" do
      writer_proc.call

      events = input(conf) do |pipeline, queue|
        queue.size.times.collect { queue.pop }
      end

      expect(events.size).to eq(event_count)

      e1_message = events[0]["message"]
      e2_message = events[1]["message"]

      # can't assume File A will be read first
      if e1_message.start_with?('line1.1-of-z')
        expect(e1_message).to eq("line1.1-of-z#{FILE_DELIMITER}  line1.2-of-z#{FILE_DELIMITER}  line1.3-of-z")
        expect(e2_message).to eq("line1.1-of-a#{FILE_DELIMITER}  line1.2-of-a#{FILE_DELIMITER}  line1.3-of-a")
      else
        expect(e1_message).to eq("line1.1-of-a#{FILE_DELIMITER}  line1.2-of-a#{FILE_DELIMITER}  line1.3-of-a")
        expect(e2_message).to eq("line1.1-of-z#{FILE_DELIMITER}  line1.2-of-z#{FILE_DELIMITER}  line1.3-of-z")
      end
    end
  end
end
