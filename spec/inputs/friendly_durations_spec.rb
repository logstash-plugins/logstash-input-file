# encoding: utf-8

require "helpers/spec_helper"
require "logstash/inputs/friendly_durations"

describe "FriendlyDurations module function call" do
  context "unacceptable strings" do
    it "gives an error message for 'foobar'" do
      result = LogStash::Inputs::FriendlyDurations.call("foobar","sec")
      expect(result.error_message).to start_with("Value 'foobar' is not a valid duration string e.g. 200 usec")
    end
    it "gives an error message for '5 5 days'" do
      result = LogStash::Inputs::FriendlyDurations.call("5 5 days","sec")
      expect(result.error_message).to start_with("Value '5 5 days' is not a valid duration string e.g. 200 usec")
    end
  end

  context "when a unit is not specified, a unit override will affect the result" do
    it "coerces 14 to 1209600.0s as days" do
      result = LogStash::Inputs::FriendlyDurations.call(14,"d")
      expect(result.error_message).to eq(nil)
      expect(result.value).to eq(1209600.0)
    end
    it "coerces '30' to 1800.0s as minutes" do
      result = LogStash::Inputs::FriendlyDurations.call("30","minutes")
      expect(result.to_a).to eq([true, 1800.0])
    end
  end

  context "acceptable strings" do
    [
      ["10",                10.0],
      ["10.5 s",            10.5],
      ["10.75 secs",        10.75],
      ["11 second",         11.0],
      ["10 seconds",        10.0],
      ["500 ms",             0.5],
      ["750.9 msec",         0.7509],
      ["750.9 msecs",        0.7509],
      ["750.9 us",           0.0007509],
      ["750.9 usec",         0.0007509],
      ["750.9 usecs",        0.0007509],
      ["1.5m",              90.0],
      ["2.5 m",            150.0],
      ["1.25 min",          75.0],
      ["1 minute",          60.0],
      ["2.5 minutes",      150.0],
      ["2h",              7200.0],
      ["2 h",             7200.0],
      ["1 hour",          3600.0],
      ["1hour",           3600.0],
      ["3 hours",        10800.0],
      ["0.5d",           43200.0],
      ["1day",           86400.0],
      ["1 day",          86400.0],
      ["2days",         172800.0],
      ["14 days",      1209600.0],
      ["1w",            604800.0],
      ["1 w",           604800.0],
      ["1 week",        604800.0],
      ["2weeks",       1209600.0],
      ["2 weeks",      1209600.0],
      ["1.5 weeks",     907200.0],
    ].each do |input, coerced|
      it "coerces #{input.inspect.rjust(16)} to #{coerced.inspect}" do
        result = LogStash::Inputs::FriendlyDurations.call(input,"sec")
        expect(result.to_a).to eq([true, coerced])
      end
    end
  end
end
