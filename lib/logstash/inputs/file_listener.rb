# encoding: utf-8

module LogStash module Inputs
  # As and when a new WatchedFile is processed FileWatch asks for an instance of this class for the
  # file path of that WatchedFile. All subsequent callbacks are sent via this listener instance.
  # The file is essentially a stream and the path is the identity of that stream.
  class FileListener
    attr_reader :input, :path, :data
    # construct with link back to the input plugin instance.
    def initialize(path, input, data = nil)
      @path, @input = path, input
      @data = data
    end

    def opened
    end

    def eof
    end

    def error
    end

    def reading_completed
    end

    def timed_out
      input.codec.evict(path)
    end

    def deleted
      input.codec.evict(path)
      input.handle_deletable_path(path)
    end

    def accept(data)
      # and push transient data filled dup listener downstream
      input.log_line_received(path, data)
      input.codec.accept(self.class.new(path, input, data))
    end

    def process_event(event)
      input.post_process_this(event, path)
    end

  end

  class FlushableListener < FileListener
    attr_writer :path
  end
end end
