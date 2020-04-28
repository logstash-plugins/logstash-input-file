# encoding: utf-8

require "helpers/spec_helper"
require "logstash/inputs/file"

# LogStash::Logging::Logger::configure_logging("DEBUG")

require "tempfile"
require "stud/temporary"
require "logstash/codecs/multiline"

describe LogStash::Inputs::File do
  describe "'read' mode testing with input(conf) do |pipeline, queue|" do
    it "should start at the beginning of an existing file and delete the file when done" do
      directory = Stud::Temporary.directory
      tmpfile_path = ::File.join(directory, "A.log")
      sincedb_path = ::File.join(directory, "readmode_A_sincedb.txt")
      path_path = ::File.join(directory, "*.log")

      conf = <<-CONFIG
        input {
          file {
            id => "blah"
            path => "#{path_path}"
            sincedb_path => "#{sincedb_path}"
            delimiter => "|"
            mode => "read"
            file_completed_action => "delete"
          }
        }
      CONFIG

      File.open(tmpfile_path, "a") do |fd|
        fd.write("hello|world")
        fd.fsync
      end

      events = input(conf) do |pipeline, queue|
        wait(0.5).for{File.exist?(tmpfile_path)}.to be_falsey
        2.times.collect { queue.pop }
      end

      expect(events.map{|e| e.get("message")}).to contain_exactly("hello", "world")
    end

    it "should start at the beginning of an existing file and log the file when done" do
      directory = Stud::Temporary.directory
      tmpfile_path = ::File.join(directory, "A.log")
      sincedb_path = ::File.join(directory, "readmode_A_sincedb.txt")
      path_path = ::File.join(directory, "*.log")
      log_completed_path = ::File.join(directory, "A_completed.txt")

      conf = <<-CONFIG
        input {
          file {
            id => "blah"
            path => "#{path_path}"
            sincedb_path => "#{sincedb_path}"
            delimiter => "|"
            mode => "read"
            file_completed_action => "log"
            file_completed_log_path => "#{log_completed_path}"
          }
        }
      CONFIG

      File.open(tmpfile_path, "a") do |fd|
        fd.write("hello|world")
        fd.fsync
      end

      events = input(conf) do |pipeline, queue|
        wait(0.75).for { IO.read(log_completed_path) }.to match(/A\.log/)
        2.times.collect { queue.pop }
      end
      expect(events.map{|e| e.get("message")}).to contain_exactly("hello", "world")
    end

    it "should read whole file when exit_after_read is set to true" do
      directory = Stud::Temporary.directory
      tmpfile_path = ::File.join(directory, "B.log")
      sincedb_path = ::File.join(directory, "readmode_B_sincedb.txt")
      path_path = ::File.join(directory, "*.log")

      conf = <<-CONFIG
        input {
          file {
            id => "foo"
            path => "#{path_path}"
            sincedb_path => "#{sincedb_path}"
            delimiter => "|"
            mode => "read"
            file_completed_action => "delete"
            exit_after_read => true
          }
        }
      CONFIG

      File.open(tmpfile_path, "a") do |fd|
        fd.write("exit|after|end")
        fd.fsync
      end

      events = input(conf) do |pipeline, queue|
        wait(0.5).for{File.exist?(tmpfile_path)}.to be_falsey
        3.times.collect { queue.pop }
      end

      expect(events.map{|e| e.get("message")}).to contain_exactly("exit", "after", "end")
    end

  end

  describe "reading fixtures" do
    let(:fixture_dir) { Pathname.new(FileInput::FIXTURE_DIR).expand_path }

    context "for a file without a final newline character" do
      let(:file_path) { fixture_dir.join('no-final-newline.log') }

      it "the file is read and the path is logged to the `file_completed_log_path` file" do
        tmpfile_path = fixture_dir.join("no-f*.log")
        sincedb_path = Stud::Temporary.pathname
        FileInput.make_fixture_current(file_path.to_path)
        log_completed_path = Stud::Temporary.pathname

        conf = <<-CONFIG
        input {
          file {
            type => "blah"
            path => "#{tmpfile_path}"
            sincedb_path => "#{sincedb_path}"
            mode => "read"
            file_completed_action => "log"
            file_completed_log_path => "#{log_completed_path}"
          }
        }
        CONFIG

        events = input(conf) do |pipeline, queue|
          wait(0.75).for { IO.read(log_completed_path) }.to match(/#{file_path.to_s}/)
          2.times.collect { queue.pop }
        end

        expect(events[0].get("message")).to start_with("2010-03-12   23:51")
        expect(events[1].get("message")).to start_with("2010-03-12   23:51")
      end

    end

    context "for an uncompressed file" do
      let(:file_path) { fixture_dir.join('uncompressed.log') }

      it "the file is read and the path is logged to the `file_completed_log_path` file" do
        FileInput.make_fixture_current(file_path.to_path)
        tmpfile_path = fixture_dir.join("unc*.log")
        directory = Stud::Temporary.directory
        sincedb_path = ::File.join(directory, "readmode_B_sincedb.txt")
        log_completed_path = ::File.join(directory, "B_completed.txt")

        conf = <<-CONFIG
        input {
          file {
            type => "blah"
            path => "#{tmpfile_path}"
            sincedb_path => "#{sincedb_path}"
            mode => "read"
            file_completed_action => "log"
            file_completed_log_path => "#{log_completed_path}"
          }
        }
        CONFIG

        events = input(conf) do |pipeline, queue|
          wait(0.75).for{ IO.read(log_completed_path) }.to match(/uncompressed\.log/)
          2.times.collect { queue.pop }
        end

        expect(events[0].get("message")).to start_with("2010-03-12   23:51")
        expect(events[1].get("message")).to start_with("2010-03-12   23:51")
      end
    end

    context "for a compressed file" do
      it "the file is read" do
        file_path = fixture_dir.join('compressed.log.gz')
        file_path2 = fixture_dir.join('compressed.log.gzip')
        FileInput.make_fixture_current(file_path.to_path)
        FileInput.make_fixture_current(file_path2.to_path)
        tmpfile_path = fixture_dir.join("compressed.*.*")
        directory = Stud::Temporary.directory
        sincedb_path = ::File.join(directory, "readmode_C_sincedb.txt")
        log_completed_path = ::File.join(directory, "C_completed.txt")

        conf = <<-CONFIG
        input {
          file {
            type => "blah"
            path => "#{tmpfile_path}"
            sincedb_path => "#{sincedb_path}"
            mode => "read"
            file_completed_action => "log"
            file_completed_log_path => "#{log_completed_path}"
          }
        }
        CONFIG

        events = input(conf) do |pipeline, queue|
          wait(0.75).for { IO.read(log_completed_path).scan(/compressed\.log\.gz(ip)?/).size }.to eq(2)
          4.times.collect { queue.pop }
        end

        expect(events[0].get("message")).to start_with("2010-03-12   23:51")
        expect(events[1].get("message")).to start_with("2010-03-12   23:51")
        expect(events[2].get("message")).to start_with("2010-03-12   23:51")
        expect(events[3].get("message")).to start_with("2010-03-12   23:51")
      end

      it "the corrupted file is untouched" do
        directory = Stud::Temporary.directory
        file_path = fixture_dir.join('compressed.log.gz')
        corrupted_file_path = ::File.join(directory, 'corrupted.gz')
        FileUtils.cp(file_path, corrupted_file_path)

        FileInput.corrupt_gzip(corrupted_file_path)

        log_completed_path = ::File.join(directory, "C_completed.txt")
        f = File.new(log_completed_path, "w")
        f.close()

        conf = <<-CONFIG
        input {
          file {
            type => "blah"
            path => "#{corrupted_file_path}"
            mode => "read"
            file_completed_action => "log_and_delete"
            file_completed_log_path => "#{log_completed_path}"
            check_archive_validity => true
          }
        }
        CONFIG

        events = input(conf) do |pipeline, queue|
          wait(1)
          expect(IO.read(log_completed_path)).to be_empty
        end
      end
    end
  end

  let(:temp_directory) { Stud::Temporary.directory }
  let(:interval) { 0.1 }
  let(:options) do
    {
        'mode' => "read",
        'path' => "#{temp_directory}/*",
        'stat_interval' => interval,
        'discover_interval' => interval,
        'sincedb_path' => "#{temp_directory}/.sincedb",
        'sincedb_write_interval' => interval
    }
  end

  let(:queue) { Queue.new }
  let(:plugin) { LogStash::Inputs::File.new(options) }

  describe 'delete on complete' do

    let(:options) do
      super.merge({ 'file_completed_action' => "delete", 'exit_after_read' => false })
    end

    let(:sample_file) { File.join(temp_directory, "sample.log") }

    before do
      plugin.register
      @run_thread = Thread.new(plugin) do |plugin|
        Thread.current.abort_on_exception = true
        plugin.run queue
      end

      File.open(sample_file, 'w') { |fd| fd.write("sample-content\n") }

      wait_for_start_processing(@run_thread)
    end

    after { plugin.stop }

    it 'processes a file' do
      wait_for_file_removal(sample_file) # watched discovery

      expect( plugin.queue.size ).to eql 1
      event = plugin.queue.pop
      expect( event.get('message') ).to eql 'sample-content'
    end

    it 'removes watched file from collection' do
      wait_for_file_removal(sample_file) # watched discovery

      watched_files = plugin.watcher.watch.watched_files_collection
      expect( watched_files ).to be_empty
    end

    private

    def wait_for_start_processing(run_thread, timeout: 1.0)
      begin
        Timeout.timeout(timeout) do
          sleep(0.01) while run_thread.status != 'sleep'
        end
      rescue Timeout::Error
        raise "plugin did not start processing (timeout: #{timeout})" unless plugin.queue
      else
        raise "plugin did not start processing" unless plugin.queue
      end
    end

    def wait_for_file_removal(path, timeout: 3 * interval)
      wait(timeout).for { File.exist?(path) }.to be_falsey
      raise "plugin did not start processing" unless plugin.queue
    end

  end
end
