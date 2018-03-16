require 'stud/temporary'
require_relative 'spec_helper'

module FileWatch
  describe WatchedFile do
    context 'Given two instances of the same file' do
      let(:pathname) { Pathname.new(__FILE__) }
      it 'theirs sincedb_keys should equate' do
        hash_db = Hash.new

        wf1 = WatchedFile.new(pathname, pathname.stat, Settings.new)
        hash_db[wf1.sincedb_key] = 42
        wf2 = WatchedFile.new(pathname, pathname.stat, Settings.new)
        expect(wf1.sincedb_key).to eq(wf2.sincedb_key)
        expect(wf1.sincedb_key).to eql(wf2.sincedb_key)
        expect(wf1.sincedb_key.hash).to eq(wf2.sincedb_key.hash)
        expect(hash_db[wf2.sincedb_key]).to eq(42)
      end
    end
  end
end
