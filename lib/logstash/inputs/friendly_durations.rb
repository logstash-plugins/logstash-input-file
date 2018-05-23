# encoding: utf-8

module LogStash module Inputs
  module FriendlyDurations
    NUMBERS_RE = /^(?<number>\d+(\.\d+)?)\s?(?<units>s((ec)?(ond)?)(s)?|m((in)?(ute)?)(s)?|h(our)?(s)?|d(ay)?(s)?|w(eek)?(s)?|us(ec)?(s)?|ms(ec)?(s)?)?$/
    HOURS = 3600
    DAYS = 24 * HOURS
    MEGA = 10**6
    KILO = 10**3

    def self.call(value, unit = "sec")
      val_string = value.to_s.strip
      result = coerce_in_seconds(val_string, unit)
      return result if result.is_a?(String)
      yield result
      nil
    end

    private

    def self.coerce_in_seconds(value, unit)
      matched = NUMBERS_RE.match(value)
      if matched.nil?
        return "Value '#{value}' is not a valid duration string e.g. 200 usec, 250ms, 60 sec, 18h, 21.5d, 1 day, 2w, 6 weeks"
      end
      multiplier = matched[:units] || unit
      numeric = matched[:number].to_f
      case multiplier
      when "m","min","mins","minute","minutes"
        numeric * 60
      when "h","hour","hours"
        numeric * HOURS
      when "d","day","days"
        numeric * DAYS
      when "w","week","weeks"
        numeric * 7 * DAYS
      when "ms","msec","msecs"
        numeric / KILO
      when "us","usec","usecs"
        numeric / MEGA
      else
        numeric
      end
    end
  end
end end
