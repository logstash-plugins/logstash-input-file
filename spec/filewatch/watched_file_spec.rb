require 'stud/temporary'
require_relative 'spec_helper'

module FileWatch
  describe WatchedFile do
    context 'Given two instances of the same file' do
      let(:pathname) { Pathname.new(__FILE__) }
      it 'their sincedb_keys should equate' do
        wf_key1 = WatchedFile.new(pathname, pathname.stat, Settings.new).sincedb_key
        hash_db = { wf_key1 => 42 }
        wf_key2 = WatchedFile.new(pathname, pathname.stat, Settings.new).sincedb_key
        expect(wf_key1).to eq(wf_key2)
        expect(wf_key1).to eql(wf_key2)
        expect(wf_key1.hash).to eq(wf_key2.hash)
        expect(hash_db[wf_key2]).to eq(42)
      end
    end
  end
end
