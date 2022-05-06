# encoding: utf-8

require "helpers/spec_helper"
require "logstash/devutils/rspec/shared_examples"
require "logstash/inputs/file"
require "logstash/plugin_mixins/ecs_compatibility_support/spec_helper"

require "json"
require "tempfile"
require "stud/temporary"
require "logstash/codecs/multiline"

# LogStash::Logging::Logger::configure_logging("DEBUG")

TEST_FILE_DELIMITER = $/

describe LogStash::Inputs::File do
  describe "'tail' mode testing with input(conf) do |pipeline, queue|" do
    it_behaves_like "an interruptible input plugin" do
      let(:config) do
        {
          "path" => Stud::Temporary.pathname,
          "sincedb_path" => Stud::Temporary.pathname
        }
      end
    end

    let(:directory)    { Stud::Temporary.directory }
    let(:sincedb_dir)  { Stud::Temporary.directory }
    let(:tmpfile_path) { ::File.join(directory, "#{name}.txt") }
    let(:sincedb_path) { ::File.join(sincedb_dir, "readmode_#{name}_sincedb.txt") }
    let(:path_path)    { ::File.join(directory, "*.txt") }

    context "for an existing file" do
      let(:name) { "A" }
      it "should start at the beginning" do
        conf = <<-CONFIG
          input {
            file {
              type => "blah"
              path => "#{path_path}"
              start_position => "beginning"
              sincedb_path => "#{sincedb_path}"
              delimiter => "#{TEST_FILE_DELIMITER}"
            }
          }
        CONFIG

        File.open(tmpfile_path, "a") do |fd|
          fd.puts("hello")
          fd.puts("world")
          fd.fsync
        end

        events = input(conf) do |pipeline, queue|
          2.times.collect { queue.pop }
        end
        expect(events.map{|e| e.get("message")}).to contain_exactly("hello", "world")
      end
    end

    context "running the input twice" do
      let(:name) { "B" }
      it "should restart at the sincedb value" do
        conf = <<-CONFIG
          input {
            file {
              type => "blah"
              path => "#{path_path}"
              start_position => "beginning"
              sincedb_path => "#{sincedb_path}"
              file_sort_by => "path"
              delimiter => "#{TEST_FILE_DELIMITER}"
            }
          }
        CONFIG

        File.open(tmpfile_path, "w") do |fd|
          fd.puts("hello3")
          fd.puts("world3")
        end

        events = input(conf) do |pipeline, queue|
          2.times.collect { queue.pop }
        end

        expect(events.map{|e| e.get("message")}).to contain_exactly("hello3", "world3")

        File.open(tmpfile_path, "a") do |fd|
          fd.puts("foo")
          fd.puts("bar")
          fd.puts("baz")
          fd.fsync
        end

        events = input(conf) do |pipeline, queue|
          3.times.collect { queue.pop }
        end
        messages = events.map{|e| e.get("message")}
        expect(messages).to contain_exactly("foo", "bar", "baz")
      end
    end


    context "when path and host fields exist", :ecs_compatibility_support do
      ecs_compatibility_matrix(:disabled, :v1, :v8 => :v1) do |ecs_select|

        before(:each) do
          allow_any_instance_of(described_class).to receive(:ecs_compatibility).and_return(ecs_compatibility)
        end

        let(:file_path_target_field  ) { ecs_select[disabled: "path", v1: '[log][file][path]'] }
        let(:source_host_target_field) { ecs_select[disabled: "host", v1: '[host][name]'] }

        let(:event_with_existing) do
          LogStash::Event.new.tap do |e|
            e.set(file_path_target_field, 'my_path')
            e.set(source_host_target_field, 'my_host')
          end.to_hash
        end

        let(:name) { "C" }
        it "should not overwrite them" do
          conf = <<-CONFIG
            input {
              file {
                type => "blah"
                path => "#{path_path}"
                start_position => "beginning"
                sincedb_path => "#{sincedb_path}"
                delimiter => "#{TEST_FILE_DELIMITER}"
                codec => "json"
              }
            }
          CONFIG

          File.open(tmpfile_path, "w") do |fd|
            fd.puts(event_with_existing.to_json)
            fd.puts('{"my_field": "my_val"}')
            fd.fsync
          end

          events = input(conf) do |pipeline, queue|
            2.times.collect { queue.pop }
          end

          existing_path_index, added_path_index  = "my_val" == events[0].get("my_field") ? [1,0] : [0,1]

          expect(events[existing_path_index].get(file_path_target_field)).to eq "my_path"
          expect(events[existing_path_index].get(source_host_target_field)).to eq "my_host"
          expect(events[existing_path_index].get("[@metadata][host]")).to eq "#{Socket.gethostname.force_encoding(Encoding::UTF_8)}"

          expect(events[added_path_index].get(file_path_target_field)).to eq "#{tmpfile_path}"
          expect(events[added_path_index].get(source_host_target_field)).to eq "#{Socket.gethostname.force_encoding(Encoding::UTF_8)}"
          expect(events[added_path_index].get("[@metadata][host]")).to eq "#{Socket.gethostname.force_encoding(Encoding::UTF_8)}"
        end
      end
    end

    context "running the input twice", :ecs_compatibility_support do
      ecs_compatibility_matrix(:disabled, :v1, :v8 => :v1) do |ecs_select|

        before(:each) do
          allow_any_instance_of(described_class).to receive(:ecs_compatibility).and_return(ecs_compatibility)
        end
        
        let(:file_path_target_field  ) { ecs_select[disabled: "path", v1: '[log][file][path]'] }
        let(:source_host_target_field) { ecs_select[disabled: "host", v1: '[host][name]'] }
        
        let(:name) { "D" }
        it "should read old files" do
          conf = <<-CONFIG
            input {
              file {
                type => "blah"
                path => "#{path_path}"
                start_position => "beginning"
                codec => "json"
              }
            }
          CONFIG

          File.open(tmpfile_path, "w") do |fd|
            fd.puts('{"path": "my_path", "host": "my_host"}')
            fd.puts('{"my_field": "my_val"}')
            fd.fsync
          end
          # arbitrary old file (2 days)
          FileInput.make_file_older(tmpfile_path, 48 * 60 * 60)

          events = input(conf) do |pipeline, queue|
            2.times.collect { queue.pop }
          end
          existing_path_index, added_path_index  = "my_val" == events[0].get("my_field") ? [1,0] : [0,1]
          expect(events[existing_path_index].get("path")).to eq "my_path"
          expect(events[existing_path_index].get("host")).to eq "my_host"
          expect(events[existing_path_index].get("[@metadata][host]")).to eq "#{Socket.gethostname.force_encoding(Encoding::UTF_8)}"

          expect(events[added_path_index].get(file_path_target_field)).to eq "#{tmpfile_path}"
          expect(events[added_path_index].get(source_host_target_field)).to eq "#{Socket.gethostname.force_encoding(Encoding::UTF_8)}"
          expect(events[added_path_index].get("[@metadata][host]")).to eq "#{Socket.gethostname.force_encoding(Encoding::UTF_8)}"
        end
      end
    end

    context "when sincedb_path is a directory" do
      let(:name) { "E" }
      subject { LogStash::Inputs::File.new("path" => path_path, "sincedb_path" => directory) }

      after :each do
        FileUtils.rm_rf(sincedb_path)
      end

      it "should raise exception" do
        expect { subject.register }.to raise_error(ArgumentError)
      end
    end

    context "when mode it set to tail and exit_after_read equals true" do
        subject { LogStash::Inputs::File.new("path" => path_path, "exit_after_read" => true, "mode" => "tail") }

      it "should raise exception" do
        expect { subject.register }.to raise_error(ArgumentError)
      end
    end

  end

  describe "testing with new, register, run and stop" do
    let(:suffix)       { "A" }
    let(:conf)         { Hash.new }
    let(:mlconf)       { Hash.new }
    let(:events)       { Array.new }
    let(:mlcodec)      { LogStash::Codecs::Multiline.new(mlconf) }
    let(:tracer_codec) { FileInput::CodecTracer.new }
    let(:tmpdir_path)  { Stud::Temporary.directory }
    let(:tmpfile_path) { ::File.join(tmpdir_path, "#{suffix}.txt") }
    let(:path_path)    { ::File.join(tmpdir_path, "*.txt") }
    let(:sincedb_path) { ::File.join(tmpdir_path, "sincedb-#{suffix}") }

    after :each do
      sleep(0.1) until subject.completely_stopped?
      FileUtils.rm_rf(sincedb_path)
    end

    context "when data exists and then more data is appended", :ecs_compatibility_support do
      ecs_compatibility_matrix(:disabled, :v1, :v8 => :v1) do |ecs_select|

        before(:each) do
          allow_any_instance_of(described_class).to receive(:ecs_compatibility).and_return(ecs_compatibility)
        end

        let(:file_path_target_field  ) { ecs_select[disabled: "path", v1: '[log][file][path]'] }
        subject { described_class.new(conf) }

        before do
          File.open(tmpfile_path, "w") do |fd|
            fd.puts("ignore me 1")
            fd.puts("ignore me 2")
            fd.fsync
          end
          mlconf.update("pattern" => "^\s", "what" => "previous")
          conf.update("type" => "blah",
                "path" => path_path,
                "sincedb_path" => sincedb_path,
                "stat_interval" => 0.1,
                "codec" => mlcodec,
                "delimiter" => TEST_FILE_DELIMITER)
        end

        it "reads the appended data only" do
          subject.register
          actions = RSpec::Sequencing
            .run_after(1, "append two lines after delay") do
              File.open(tmpfile_path, "a") { |fd| fd.puts("hello"); fd.puts("world") }
            end
            .then("wait for one event") do
              wait(0.75).for{events.size}.to eq(1)
            end
            .then("quit") do
              subject.stop
            end
            .then("wait for flushed event") do
              wait(0.75).for{events.size}.to eq(2)
            end

          subject.run(events)
          actions.assert_no_errors

          event1 = events[0]
          expect(event1).not_to be_nil
          expect(event1.get(file_path_target_field)).to eq tmpfile_path
          expect(event1.get("[@metadata][path]")).to eq tmpfile_path
          expect(event1.get("message")).to eq "hello"

          event2 = events[1]
          expect(event2).not_to be_nil
          expect(event2.get(file_path_target_field)).to eq tmpfile_path
          expect(event2.get("[@metadata][path]")).to eq tmpfile_path
          expect(event2.get("message")).to eq "world"
        end
      end
    end

    context "when close_older config is specified" do
      let(:line)         { "line1.1-of-a" }
      let(:suffix)       { "X" }
      subject { described_class.new(conf) }

      before do
        conf.update(
              "type" => "blah",
              "path" => path_path,
              "sincedb_path" => sincedb_path,
              "stat_interval" => 0.02,
              "codec" => tracer_codec,
              "close_older" => "100 ms",
              "start_position" => "beginning",
              "delimiter" => TEST_FILE_DELIMITER)

        subject.register
      end

      it "having timed_out, the codec is auto flushed" do
        actions = RSpec::Sequencing
          .run("create file") do
            File.open(tmpfile_path, "wb") { |file|  file.puts(line) }
          end
          .then_after(0.1, "identity is mapped") do
            wait(0.75).for{subject.codec.identity_map[tmpfile_path]}.not_to be_nil, "identity is not mapped"
          end
          .then("wait accept") do
            wait(0.75).for {
              subject.codec.identity_map[tmpfile_path].codec.trace_for(:accept)
            }.to eq(true), "accept didn't"
          end
          .then("request a stop") do
            # without this the subject.run doesn't invokes the #exit_flush which is the only @codec.flush_mapped invocation
            subject.stop
          end
          .then("wait for auto_flush") do
            wait(2).for {
              subject.codec.identity_map[tmpfile_path].codec.trace_for(:auto_flush)
            }.to eq(true), "autoflush didn't"
          end
        subject.run(events)
        actions.assert_no_errors
        expect(subject.codec.identity_map[tmpfile_path].codec.trace_for(:accept)).to eq(true)
      end
    end

    context "when ignore_older config is specified" do
      let(:suffix) { "Y" }
      before do
        conf.update(
              "type" => "blah",
              "path" => path_path,
              "sincedb_path" => sincedb_path,
              "stat_interval" => 0.02,
              "codec" => tracer_codec,
              "ignore_older" => "500 ms",
              "delimiter" => TEST_FILE_DELIMITER)
      end
      subject { described_class.new(conf) }
      let(:line) { "line1.1-of-a" }

      it "the file is not read" do
        subject.register
        RSpec::Sequencing
          .run("create file") do
            File.open(tmp_dir_file, "a") do |fd|
              fd.puts(line)
              fd.fsync
            end
            FileInput.make_file_older(tmp_dir_file, 2)
          end
          .then_after(0.5, "stop") do
            subject.stop
          end
        subject.run(events)
        expect(subject.codec.identity_map[tmpfile_path].codec.trace_for(:accept)).to be_falsey
      end
    end

    context "when wildcard path and a multiline codec is specified", :ecs_compatibility_support do
      ecs_compatibility_matrix(:disabled, :v1, :v8 => :v1) do |ecs_select|

        before(:each) do
          allow_any_instance_of(described_class).to receive(:ecs_compatibility).and_return(ecs_compatibility)
        end

        let(:file_path_target_field  ) { ecs_select[disabled: "path", v1: '[log][file][path]'] }

        subject { described_class.new(conf) }
        let(:suffix)       { "J" }
        let(:tmpfile_path2) { ::File.join(tmpdir_path, "K.txt") }
        before do
          mlconf.update("pattern" => "^\s", "what" => "previous")
          conf.update(
                "type" => "blah",
                "path" => path_path,
                "start_position" => "beginning",
                "sincedb_path" => sincedb_path,
                "stat_interval" => 0.05,
                "codec" => mlcodec,
                "file_sort_by" => "path",
                "delimiter" => TEST_FILE_DELIMITER)

          subject.register
        end

        it "collects separate multiple line events from each file" do
          subject
          actions = RSpec::Sequencing
            .run_after(0.1, "create files") do
              File.open(tmpfile_path, "wb") do |fd|
                fd.puts("line1.1-of-J")
                fd.puts("  line1.2-of-J")
                fd.puts("  line1.3-of-J")
              end
              File.open(tmpfile_path2, "wb") do |fd|
                fd.puts("line1.1-of-K")
                fd.puts("  line1.2-of-K")
                fd.puts("  line1.3-of-K")
              end
            end
            .then("assert both files are mapped as identities and stop") do
              wait(2).for {subject.codec.identity_count}.to eq(2), "both files are not mapped as identities"
            end
            .then("stop") do
              subject.stop
            end
          subject.run(events)
          # wait for actions to complete
          actions.assert_no_errors
          expect(events.size).to eq(2)
          e1, e2 = events
          e1_message = e1.get("message")
          e2_message = e2.get("message")

          expect(e1.get(file_path_target_field)).to match(/J.txt/)
          expect(e2.get(file_path_target_field)).to match(/K.txt/)
          expect(e1_message).to eq("line1.1-of-J#{TEST_FILE_DELIMITER}  line1.2-of-J#{TEST_FILE_DELIMITER}  line1.3-of-J")
          expect(e2_message).to eq("line1.1-of-K#{TEST_FILE_DELIMITER}  line1.2-of-K#{TEST_FILE_DELIMITER}  line1.3-of-K")
        end

        context "if auto_flush is enabled on the multiline codec" do
          let(:mlconf) { { "auto_flush_interval" => 0.5 } }
          let(:suffix)       { "M" }
          it "an event is generated via auto_flush" do
            actions = RSpec::Sequencing
              .run_after(0.1, "create files") do
                File.open(tmpfile_path, "wb") do |fd|
                  fd.puts("line1.1-of-a")
                  fd.puts("  line1.2-of-a")
                  fd.puts("  line1.3-of-a")
                end
              end
              .then("wait for auto_flush") do
                wait(2).for{events.size}.to eq(1), "events size is not 1"
              end
              .then("stop") do
                subject.stop
              end
            subject.run(events)
            # wait for actions to complete
            actions.assert_no_errors
            e1 = events.first
            e1_message = e1.get("message")
            expect(e1_message).to eq("line1.1-of-a#{TEST_FILE_DELIMITER}  line1.2-of-a#{TEST_FILE_DELIMITER}  line1.3-of-a")
            expect(e1.get(file_path_target_field)).to match(/M.txt$/)
          end
        end
      end
    end

    describe "specifying max_open_files" do
      let(:suffix)       { "P" }
      let(:tmpfile_path2) { ::File.join(tmpdir_path, "Q.txt") }
      subject { described_class.new(conf) }
      before do
        File.open(tmpfile_path, "w") do |fd|
          fd.puts("line1-of-P")
          fd.puts("line2-of-P")
          fd.fsync
        end
        File.open(tmpfile_path2, "w") do |fd|
          fd.puts("line1-of-Q")
          fd.puts("line2-of-Q")
          fd.fsync
        end
      end

      context "when close_older is NOT specified" do
        before do
          conf.clear
          conf.update(
                "type" => "blah",
                "path" => path_path,
                "sincedb_path" => sincedb_path,
                "stat_interval" => 0.1,
                "max_open_files" => 1,
                "start_position" => "beginning",
                "file_sort_by" => "path",
                "delimiter" => TEST_FILE_DELIMITER)
          subject.register
        end
        it "collects line events from only one file" do
          actions = RSpec::Sequencing
            .run("assert one identity is mapped") do
              wait(0.4).for{subject.codec.identity_count}.to be > 0, "no identity is mapped"
            end
            .then("stop") do
              subject.stop
            end
            .then("stop flushes last event") do
              wait(0.4).for{events.size}.to eq(2), "events size does not equal 2"
            end
          subject.run(events)
          # wait for actions future value
          actions.assert_no_errors
          e1, e2 = events
          expect(e1.get("message")).to eq("line1-of-P")
          expect(e2.get("message")).to eq("line2-of-P")
        end
      end

      context "when close_older IS specified" do
        before do
          conf.update(
                "type" => "blah",
                "path" => path_path,
                "sincedb_path" => sincedb_path,
                "stat_interval" => 0.1,
                "max_open_files" => 1,
                "close_older" => 0.5,
                "start_position" => "beginning",
                "file_sort_by" => "path",
                "delimiter" => TEST_FILE_DELIMITER)
          subject.register
        end

        it "collects line events from both files" do
          actions = RSpec::Sequencing
            .run("assert both identities are mapped and the first two events are built") do
              wait(0.4).for{subject.codec.identity_count == 1 && events.size == 2}.to eq(true), "both identities are not mapped and the first two events are not built"
            end
            .then("wait for close to flush last event of each identity") do
              wait(0.8).for{events.size}.to eq(4), "close does not flush last event of each identity"
            end
            .then_after(0.1, "stop") do
              subject.stop
            end
          subject.run(events)
          # wait for actions future value
          actions.assert_no_errors
          e1, e2, e3, e4 = events
          expect(e1.get("message")).to eq("line1-of-P")
          expect(e2.get("message")).to eq("line2-of-P")
          expect(e3.get("message")).to eq("line1-of-Q")
          expect(e4.get("message")).to eq("line2-of-Q")
        end
      end
    end
  end
end
