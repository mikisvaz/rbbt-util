require 'tokyocabinet'

module Persist
  TC_CONNECTIONS = {}
  def self.open_tokyocabinet(path, write, serializer = nil)
    write = true if not File.exists?(path)
    flags = (write ? TokyoCabinet::HDB::OWRITER | TokyoCabinet::HDB::OCREAT : TokyoCabinet::HDB::OREADER)

    FileUtils.mkdir_p File.dirname(path) unless File.exists?(File.dirname(path))

    database = TC_CONNECTIONS[path] ||= TokyoCabinet::HDB.new
    database.close

    if !database.open(path, flags)
      ecode = database.ecode
      raise "Open error: #{database.errmsg(ecode)}. Trying to open file #{path}"
    end

    if not database.respond_to? :old_close
      class << database
        attr_accessor :writable, :closed, :persistence_path

        alias old_close close
        def close
          @closed = true
          old_close
        end

        def read(force = false)
          return if not write? and not closed and not force
          self.close
          if !self.open(@persistence_path, TokyoCabinet::BDB::OREADER)
            ecode = self.ecode
            raise "Open error: #{self.errmsg(ecode)}. Trying to open file #{@persistence_path}"
          end
          @writable = false
          @closed = false
          self
        end

        def write(force = true)
          return if write? and not closed and not force
          self.close
          if !self.open(@persistence_path, TokyoCabinet::HDB::OWRITER | TokyoCabinet::HDB::OCREAT)
            ecode = self.ecode
            raise "Open error: #{self.errmsg(ecode)}. Trying to open file #{@persistence_path}"
          end
          @writable = true
          @closed = false
          self
        end

        def write?
          @writable
        end

        def collect
          res = []
          each do |key, value|
            res << if block_given?
                     yield key, value
            else
              [key, value]
            end
          end
          res
        end

        def delete(key)
          out(key)
        end


        def merge!(hash)
          hash.each do |key,values|
            self[key] = values
          end
        end

      end
    end

    database.persistence_path ||= path

    TSV.setup database
    database.serializer = serializer unless serializer.nil?

    database
  end

  def self.persist_tsv(source, filename, options, persist_options = {})
    persist_options[:prefix] ||= "TSV"

    data = case
           when persist_options[:data]
             persist_options[:data]
           when persist_options[:persist]

             filename ||= source.filename if source.respond_to? :filename
             filename ||= source.object_id.to_s

             path = persistence_path(filename, persist_options, options)
             if is_persisted? path
               Log.debug "TSV persistence up-to-date: #{ path }"
               return open_tokyocabinet(path, false) 
             else
               Log.debug "TSV persistence creating: #{ path }"
             end

             FileUtils.rm path if File.exists? path

             data = open_tokyocabinet(path, true)
             data.serializer = :type
             data
           else
             data = {}
           end

    begin
      yield data
    rescue Exception
      begin
        data.close if data.respondo_to? :close
      rescue
      end
      FileUtils.rm path if path and File.exists? path
      raise $!
    end

    data.read if data.respond_to? :read and  data.respond_to? :write? and data.write?

    data
  end

end
