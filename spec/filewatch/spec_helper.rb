# encoding: utf-8
require "rspec_sequencing"
require 'rspec/wait'
require "logstash/devutils/rspec/spec_helper"
require "concurrent"
require "timecop"

def formatted_puts(text)
  cfg = RSpec.configuration
  return unless cfg.formatters.first.is_a?(
        RSpec::Core::Formatters::DocumentationFormatter)
  txt = cfg.format_docstrings_block.call(text)
  cfg.output_stream.puts "    #{txt}"
end

unless RSpec::Matchers.method_defined?(:receive_call_and_args)
  RSpec::Matchers.define(:receive_call_and_args) do |m, args|
    match do |actual|
      actual.trace_for(m) == args
     end

    failure_message do
      "Expecting method #{m} to receive: #{args} but got: #{actual.trace_for(m)}"
    end
  end
end

require_relative "../helpers/rspec_wait_handler_helper" unless defined? RSPEC_WAIT_HANDLER_PATCHED
require_relative "../helpers/logging_level_helper" unless defined? LOG_AT_HANDLED

require 'filewatch/bootstrap'

module FileWatch
  class DummyIO
    def stat
      self
    end
    def ino
      23456
    end
    def size
      65535
    end
    def mtime
      Time.now
    end
    def dev_major
      1
    end
    def dev_minor
      5
    end
  end

  class DummyFileReader
    def initialize(read_size, iterations)
      @read_size = read_size
      @iterations = iterations
      @closed = false
      @accumulated = 0
      @io = DummyIO.new
    end
    def file_seek(*)
    end
    def close()
      @closed = true
    end
    def closed?
      @closed
    end
    def to_io
      @io
    end
    def sysread(amount)
      @accumulated += amount
      if @accumulated > @read_size * @iterations
        raise EOFError.new
      end
      string = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcde\n"
      multiplier = amount / string.length
      string * multiplier
    end
    def sysseek(offset, whence)
    end
  end

  FIXTURE_DIR = File.join('spec', 'fixtures')

  def self.make_file_older(path, seconds)
    time = Time.now.to_f - seconds
    ::File.utime(time, time, path)
  end

  def self.make_fixture_current(path, time = Time.now)
    ::File.utime(time, time, path)
  end

  class TracerBase
    def initialize
      @tracer = Concurrent::Array.new
    end

    def trace_for(symbol)
      params = @tracer.map {|k,v| k == symbol ? v : nil}.compact
      params.empty? ? false : params
    end

    def clear
      @tracer.clear
    end
  end

  module NullCallable
    def self.call
    end
  end

  class TestObserver
    class Listener
      attr_reader :path, :lines, :calls

      def initialize(path, lines)
        @path = path
        @lines = lines || Concurrent::Array.new
        @calls = Concurrent::Array.new
      end

      def accept(line)
        @lines << line
        @calls << :accept
      end

      def deleted
        @calls << :delete
      end

      def opened
        @calls << :open
      end

      def error
        @calls << :error
      end

      def eof
        @calls << :eof
      end

      def timed_out
        @calls << :timed_out
      end

      def reading_completed
        @calls << :reading_completed
      end
    end

    attr_reader :listeners

    def initialize(combined_lines = nil)
      @listeners = Concurrent::Hash.new { |hash, key| hash[key] = new_listener(key, combined_lines) }
    end

    def listener_for(path)
      @listeners[path]
    end

    def clear
      @listeners.clear
    end

    private

    def new_listener(path, lines = nil)
      Listener.new(path, lines)
    end

  end
end
