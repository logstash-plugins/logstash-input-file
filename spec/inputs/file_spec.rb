# encoding: utf-8

require "logstash/devutils/rspec/spec_helper"
require "tempfile"
require "stud/temporary"

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
end
