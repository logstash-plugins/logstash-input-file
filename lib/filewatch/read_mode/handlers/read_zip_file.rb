# encoding: utf-8
require 'java'

module FileWatch module ReadMode module Handlers

  java_import java.io.InputStream
  java_import java.io.InputStreamReader
  java_import java.io.FileInputStream
  java_import java.io.BufferedReader
  java_import java.util.zip.GZIPInputStream
  java_import java.util.zip.ZipException

  class ReadZipFile < Base
    def handle_specifically(watched_file)
      add_or_update_sincedb_collection(watched_file) unless sincedb_collection.member?(watched_file.sincedb_key)
      # can't really stripe read a zip file, its all or nothing.
      watched_file.listener.opened
      # what do we do about quit when we have just begun reading the zipped file (e.g. pipeline reloading)
      # should we track lines read in the sincedb and
      # fast forward through the lines until we reach unseen content?
      # meaning that we can quit in the middle of a zip file
      key = watched_file.sincedb_key

      if @settings.check_archive_validity && corrupted?(watched_file)
        watched_file.unwatch
      else
        begin
          file_stream = FileInputStream.new(watched_file.path)
          gzip_stream = GZIPInputStream.new(file_stream)
          decoder = InputStreamReader.new(gzip_stream, "UTF-8")
          buffered = BufferedReader.new(decoder)
          while (line = buffered.readLine())
            watched_file.listener.accept(line)
            # can't quit, if we did then we would incorrectly write a 'completed' sincedb entry
            # what do we do about quit when we have just begun reading the zipped file (e.g. pipeline reloading)
            # should we track lines read in the sincedb and
            # fast forward through the lines until we reach unseen content?
            # meaning that we can quit in the middle of a zip file
          end
          watched_file.listener.eof
        rescue ZipException => e
          logger.error("Cannot decompress the gzip file at path: #{watched_file.path}", :exception => e.class,
                       :message => e.message, :backtrace => e.backtrace)
          watched_file.listener.error
        else
          sincedb_collection.store_last_read(key, watched_file.last_stat_size)
          sincedb_collection.request_disk_flush
          watched_file.listener.deleted
          watched_file.unwatch
        ensure
          # rescue each close individually so all close attempts are tried
          close_and_ignore_ioexception(buffered) unless buffered.nil?
          close_and_ignore_ioexception(decoder) unless decoder.nil?
          close_and_ignore_ioexception(gzip_stream) unless gzip_stream.nil?
          close_and_ignore_ioexception(file_stream) unless file_stream.nil?
        end
      end
      sincedb_collection.clear_watched_file(key)
    end

    private

    def close_and_ignore_ioexception(closeable)
      begin
        closeable.close
      rescue Exception => e # IOException can be thrown by any of the Java classes that implement the Closable interface.
        logger.warn("Ignoring an IOException when closing an instance of #{closeable.class.name}",
                    :exception => e.class, :message => e.message, :backtrace => e.backtrace)
      end
    end

    def corrupted?(watched_file)
      begin
        start = Time.new
        file_stream = FileInputStream.new(watched_file.path)
        gzip_stream = GZIPInputStream.new(file_stream)
        buffer = Java::byte[8192].new
        until gzip_stream.read(buffer) == -1
        end
        return false
      rescue ZipException, Java::JavaIo::EOFException => e
        duration = Time.now - start
        logger.warn("Detected corrupted archive #{watched_file.path} file won't be processed", :message => e.message,
                    :duration => duration.round(3))
        return true
      ensure
        close_and_ignore_ioexception(gzip_stream) unless gzip_stream.nil?
        close_and_ignore_ioexception(file_stream) unless file_stream.nil?
      end
    end
  end
end end end
