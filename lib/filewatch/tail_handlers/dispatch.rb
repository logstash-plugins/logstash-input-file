# encoding: utf-8
require_relative "create"
require_relative "create_initial"
require_relative "delete"
require_relative "grow"
require_relative "shrink"
require_relative "timeout"
require_relative "unignore"

# Must handle
#   :create_initial - file seen in first discovery
#   :create - file seen in ongoing discovery
#   :grow   - file has more content
#   :shrink - file has less content
#   :delete   - file can't be read
#   :timeout - file is closable
#   :unignore - file is ignored, but since then it received new content
#
module FileWatch module TailHandlers
  class Dispatch
    def initialize(sincedb_collection, observer)
      @create = Create.new(sincedb_collection, observer)
      @create_initial = CreateInitial.new(sincedb_collection, observer)
      @grow = Grow.new(sincedb_collection, observer)
      @shrink = Shrink.new(sincedb_collection, observer)
      @delete = Delete.new(sincedb_collection, observer)
      @timeout = Timeout.new(sincedb_collection, observer)
      @unignore = Unignore.new(sincedb_collection, observer)
    end

    def create(watched_file)
      @create.handle(watched_file)
    end

    def create_initial(watched_file)
      @create_initial.handle(watched_file)
    end

    def grow(watched_file)
      @grow.handle(watched_file)
    end

    def shrink(watched_file)
      @shrink.handle(watched_file)
    end

    def delete(watched_file)
      @delete.handle(watched_file)
    end

    def timeout(watched_file)
      @timeout.handle(watched_file)
    end

    def unignore(watched_file)
      @unignore.handle(watched_file)
    end
  end
end end
