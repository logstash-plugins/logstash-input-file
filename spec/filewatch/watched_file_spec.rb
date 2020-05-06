# encoding: utf-8
require 'stud/temporary'
require_relative 'spec_helper'

module FileWatch
  describe WatchedFile do
    let(:pathname) { Pathname.new(__FILE__) }

    context 'Given two instances of the same file' do
      it 'their sincedb_keys should equate' do
        wf_key1 = WatchedFile.new(pathname, PathStatClass.new(pathname), Settings.new).sincedb_key
        hash_db = { wf_key1 => 42 }
        wf_key2 = WatchedFile.new(pathname, PathStatClass.new(pathname), Settings.new).sincedb_key
        expect(wf_key1).to eq(wf_key2)
        expect(wf_key1).to eql(wf_key2)
        expect(wf_key1.hash).to eq(wf_key2.hash)
        expect(hash_db[wf_key2]).to eq(42)
      end
    end

    context 'Given a barrage of state changes' do
      it 'only the previous N state changes are remembered' do
        watched_file = WatchedFile.new(pathname, PathStatClass.new(pathname), Settings.new)
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

    context 'restat' do

      let(:directory) { Stud::Temporary.directory }
      let(:file_path) { ::File.join(directory, "restat.file.txt") }
      let(:pathname) { Pathname.new(file_path) }

      before { FileUtils.touch file_path, :mtime => Time.now - 300 }

      it 'reports false value when no changes' do
        file = WatchedFile.new(pathname, PathStatClass.new(pathname), Settings.new)
        mtime = file.modified_at
        expect( file.modified_at_changed? ).to be false
        expect( file.restat! ).to be_falsy
        expect( file.modified_at_changed? ).to be false
        expect( file.modified_at ).to eql mtime
        expect( file.modified_at(true) ).to eql mtime
      end

      it 'reports truthy when changes detected' do
        file = WatchedFile.new(pathname, PathStatClass.new(pathname), Settings.new)
        mtime = file.modified_at
        expect( file.modified_at_changed? ).to be false
        FileUtils.touch file_path
        expect( file.restat! ).to be_truthy
        expect( file.modified_at_changed? ).to be true
        expect( file.modified_at ).to eql mtime # until updated
        expect( file.modified_at(true) ).to be > mtime
      end
    end
  end
end
