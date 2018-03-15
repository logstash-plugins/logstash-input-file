require "rspec_sequencing"
require "logstash/devutils/rspec/spec_helper"
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

require 'filewatch/bootstrap'

module FileWatch

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
      @tracer = []
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

      def initialize(path)
        @path = path
        @lines = []
        @calls = []
      end

      def add_lines(lines)
        @lines = lines
        self
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
    end

    attr_reader :listeners

    def initialize(combined_lines = nil)
      listener_proc = if combined_lines.nil?
        lambda{|k| Listener.new(k) }
      else
        lambda{|k| Listener.new(k).add_lines(combined_lines) }
      end
      @listeners = Hash.new {|hash, key| hash[key] = listener_proc.call(key) }
    end

    def listener_for(path)
      @listeners[path]
    end

    def clear
      @listeners.clear; end
  end
end
