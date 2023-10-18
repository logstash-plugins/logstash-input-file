# encoding: utf-8

require "logstash/devutils/rspec/spec_helper"
require "rspec/wait"
require "rspec_sequencing"

module FileInput

  FIXTURE_DIR = File.join('spec', 'fixtures')

  def self.make_file_older(path, seconds)
    time = Time.now.to_f - seconds
    ::File.utime(time, time, path)
  end

  def self.make_fixture_current(path, time = Time.now)
    ::File.utime(time, time, path)
  end

  def self.corrupt_gzip(file_path)
    f = File.open(file_path, "w")
    f.seek(12)
    f.puts 'corrupting_string'
    f.close()
  end

  def self.truncate_gzip(file_path)
    f = File.open(file_path, "ab")
    f.truncate(100)
    f.close()
  end

  class TracerBase
    def initialize
      @tracer = Concurrent::Array.new
    end

    def trace_for(symbol)
      params = @tracer.map {|k,v| k == symbol ? v : nil}.compact
      if params.empty?
        false
      else
        # merge all params with same key
        # there could be multiple instances of same call, e.g. [[:accept, true], [:auto_flush, true], [:close, true], [:auto_flush, true]]
        params.reduce {|b1, b2| b1 and b2}
      end
    end

    def clear
      @tracer.clear
    end
  end

  class CodecTracer < TracerBase
    def decode_accept(ctx, data, listener)
      @tracer.push [:decode_accept, [ctx, data]]
      listener.process(ctx, {"message" => data})
    end
    def accept(listener)
      @tracer.push [:accept, true]
    end
    def auto_flush(*)
      @tracer.push [:auto_flush, true]
    end
    def flush(*)
      @tracer.push [:flush, true]
    end
    def close
      @tracer.push [:close, true]
    end
    def clone
      self
    end
  end
end

require_relative "rspec_wait_handler_helper" unless defined? RSPEC_WAIT_HANDLER_PATCHED
require_relative "logging_level_helper" unless defined? LOG_AT_HANDLED

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

ENV["LOG_AT"].tap do |level|
  LogStash::Logging::Logger::configure_logging(level) unless level.nil?
end
