require 'logstash/devutils/rspec/spec_helper'
require 'logstash/inputs/friendly_durations'

describe FileWatch::Settings do

  context "when create from options" do
    it "doesn't convert sincedb_clean_after to seconds" do
      res = FileWatch::Settings.from_options({:sincedb_clean_after => LogStash::Inputs::FriendlyDurations.call(1, "days").value})

      expect(res.sincedb_expiry_duration).to eq 1 * 24 * 3600
    end
  end

end
