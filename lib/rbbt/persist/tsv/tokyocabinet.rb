require 'tokyocabinet'

module Persist
  TC_CONNECTIONS = {}

  module TCAdapter
    attr_accessor :persistence_path, :tokyocabinet_class, :closed

    def prefix(key)
      range(key, 1, key + 255.chr, 1)
    end

    def get_prefix(key)
      keys = prefix(key)
      select(:key => keys)
    end

    def closed?
      @closed
    end

    def close
      @closed = true
      super
    end

    def read(force = false)
      return if not write? and not closed and not force
      self.close
      if !self.open(@persistence_path, tokyocabinet_class::OREADER)
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

      if !self.open(@persistence_path, tokyocabinet_class::OWRITER)
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

    def write_and_read
      lock_filename = Persist.persistence_path(persistence_path, {:dir => TSV.lock_dir})
      Misc.lock(lock_filename) do
        write if @closed or not write?
        res = begin
                yield
              ensure
                read
              end
        res
      end
    end

    def write_and_close
      lock_filename = Persist.persistence_path(persistence_path, {:dir => TSV.lock_dir})
      Misc.lock(lock_filename) do
        write if @closed or not write?
        res = begin
                yield
              ensure
                close
              end
        res
      end
    end

    def read_and_close
      read if @closed or write?
      res = begin
              yield
            ensure
              close
            end
      res
    end

    def merge!(hash)
      hash.each do |key,values|
        self[key] = values
      end
    end


    def range(*args)
      keys = super(*args)
      keys - TSV::ENTRY_KEYS
    end
  end

  def self._open_tokyocabinet(path, write, serializer = nil, tokyocabinet_class = TokyoCabinet::HDB)
    tokyocabinet_class = TokyoCabinet::HDB if tokyocabinet_class == "HDB"
    tokyocabinet_class = TokyoCabinet::BDB if tokyocabinet_class == "BDB"

    database = TC_CONNECTIONS[path] ||= tokyocabinet_class.new

    flags = (write ? tokyocabinet_class::OWRITER | tokyocabinet_class::OCREAT : tokyocabinet_class::OREADER)
    database.close

    if !database.open(path, flags)
      ecode = database.ecode
      raise "Open error: #{database.errmsg(ecode)}. Trying to open file #{path}"
    end

    database.extend Persist::TCAdapter unless Persist::TCAdapter === database
    database.persistence_path ||= path
    database.tokyocabinet_class = tokyocabinet_class

    database
  end

  def self.open_tokyocabinet(path, write, serializer = nil, tokyocabinet_class = TokyoCabinet::HDB)
    write = true unless File.exists? path

    FileUtils.mkdir_p File.dirname(path) unless File.exists?(File.dirname(path))

    database = _open_tokyocabinet(path, write, serializer, tokyocabinet_class)

    unless serializer == :clean
      TSV.setup database
      database.serializer = serializer || database.serializer
    end

    database
  end

end
