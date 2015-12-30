# encoding: utf-8
require "logstash/inputs/component"
require "logstash/codecs/identity_map_codec"

module LogStash module Inputs class IdentityMapCodecComponent
  include Component

  attr_reader :codec

  def add_codec(codec)
    @codec = LogStash::Codecs::IdentityMapCodec.new(codec)
    self
  end

  def stop
    @codec.close
  end

  def do_work(context, data)
    do_line(context, data) || do_eviction(context, data)
  end

  def process(context, data)
    # data should be an event
    deliver(context, data)
  end

  private

  def do_line(context, data)
    return false unless line?(context)
    @codec.decode_accept(context, data, self)
    # above should call back on #process
    true
  end

  def do_eviction(context, data)
    return false unless evicting?(context)
    path = context[:path]
    @codec.evict(path) if path
    true
  end

  def line?(ctx)
    action(ctx) == "line"
  end

  def evicting?(ctx)
    _action = action(ctx)
    _action == "timed_out" || _action == "deleted"
  end

  def action(ctx)
    ctx[:action]
  end
end end end
