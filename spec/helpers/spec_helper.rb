# encoding: utf-8

require "logstash/devutils/rspec/spec_helper"
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
      self.class.new
    end
  end
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

