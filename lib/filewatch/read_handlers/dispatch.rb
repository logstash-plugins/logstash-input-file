# encoding: utf-8

require_relative "read_file"
require_relative "read_zip_file"

# Must handle
#   :read_file
#   :read_zip_file

module FileWatch module ReadHandlers
  class Dispatch
    def initialize(sincedb_collection, observer, settings)
      @read_file = ReadFile.new(sincedb_collection, observer, settings)
      @read_zip_file = ReadZipFile.new(sincedb_collection, observer, settings)
    end

    def read_file(watched_file)
      @read_file.handle(watched_file)
    end

    def read_zip_file(watched_file)
      @read_zip_file.handle(watched_file)
    end
  end
end end
