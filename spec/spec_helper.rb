# encoding: utf-8

require "logstash/devutils/rspec/spec_helper"
require "rspec_sequencing"

module FileInput
  def self.make_file_older(path, seconds)
    time = Time.now.to_f - seconds
    File.utime(time, time, path)
  end
  
  class TracerBase
    def initialize() @tracer = []; end

    def trace_for(symbol)
      params = @tracer.map {|k,v| k == symbol ? v : nil}.compact
      params.empty? ? false : params
    end

    def clear()
      @tracer.clear()
    end
  end

  class FileLogTracer < TracerBase
    def warn(*args) @tracer.push [:warn, args]; end
    def error(*args) @tracer.push [:error, args]; end
    def debug(*args) @tracer.push [:debug, args]; end
    def info(*args) @tracer.push [:info, args]; end

    def info?() true; end
    def debug?() true; end
    def warn?() true; end
    def error?() true; end
  end

  class ComponentTracer < TracerBase
    def accept(*args) @tracer.push [:accept, args]; end
    def deliver(*args) @tracer.push [:deliver, args]; end
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

unless Kernel.method_defined?(:pause_until)
  module Kernel
    def pause_until(nap = 5, &block)
      sq = SizedQueue.new(1)
      th1 = Thread.new(sq) {|q| sleep nap; q.push(false) }
      th2 = Thread.new(sq) do |q|
        success = false
        iters = nap * 5 + 1
        iters.times do
          break if !!(success = block.call)
          sleep(0.2)
        end
        q.push(success)
      end
      sq.pop
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

