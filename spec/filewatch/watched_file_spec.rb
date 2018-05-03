# encoding: utf-8
require 'stud/temporary'
require_relative 'spec_helper'

module FileWatch
  describe WatchedFile do
    let(:pathname) { Pathname.new(__FILE__) }

    context 'Given two instances of the same file' do
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

    context 'Given a barrage of state changes' do
      it 'only the previous N state changes are remembered' do
        watched_file = WatchedFile.new(pathname, pathname.stat, Settings.new)
        watched_file.ignore
        watched_file.watch
        watched_file.activate
        watched_file.watch
        watched_file.close
        watched_file.watch
        watched_file.activate
        watched_file.unwatch
        watched_file.activate
        watched_file.close
        expect(watched_file.closed?).to be_truthy
        expect(watched_file.recent_states).to eq([:watched, :active, :watched, :closed, :watched, :active, :unwatched, :active])
      end
    end
  end
end
