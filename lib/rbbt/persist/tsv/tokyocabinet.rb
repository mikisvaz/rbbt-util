require 'tokyocabinet'

module Persist

  module TCAdapter
    attr_accessor :persistence_path, :tokyocabinet_class, :closed, :writable, :mutex

    def self.open(path, write, tokyocabinet_class = TokyoCabinet::HDB)
      tokyocabinet_class = TokyoCabinet::HDB if tokyocabinet_class == "HDB"
      tokyocabinet_class = TokyoCabinet::BDB if tokyocabinet_class == "BDB"

      database = CONNECTIONS[path] ||= tokyocabinet_class.new

      flags = (write ? tokyocabinet_class::OWRITER | tokyocabinet_class::OCREAT : tokyocabinet_class::OREADER)
      database.close 

      if !database.open(path, flags)
        ecode = database.ecode
        raise "Open error: #{database.errmsg(ecode)}. Trying to open file #{path}"
      end

      database.extend Persist::TCAdapter unless Persist::TCAdapter === database
      database.persistence_path ||= path
      database.tokyocabinet_class = tokyocabinet_class

      database.mutex = Mutex.new
      database
    end

    MAX_CHAR = 255.chr

    def prefix(key)
      range(key, 1, key + MAX_CHAR, 1)
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

    def read?
      ! write?
    end
    #def each
    #  iterinit
    #  while key = iternext
    #    yield key, get(key)
    #  end
    #end

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
      lock_filename = Persist.persistence_path(persistence_path + '.write', {:dir => TSV.lock_dir})
      Misc.lock(lock_filename) do
        @mutex.synchronize do
          write if @closed or not write?
          res = begin
                  yield
                ensure
                  read
                end
          res
        end
      end
    end

    def write_and_close
      lock_filename = Persist.persistence_path(persistence_path + '.write', {:dir => TSV.lock_dir})
      Misc.lock(lock_filename) do
        @mutex.synchronize do
          write if @closed or not write?
          res = begin
                  yield
                ensure
                  close
                end
          res
        end
      end
    end

    def read_and_close
      @mutex.synchronize do
        read if @closed or not read?
        res = begin
                yield
              ensure
                close
              end
        res
      end
    end


    def merge!(hash)
      hash.each do |key,values|
        self[key] = values
      end
    end


    def range(*args)
      super(*args) #- TSV::ENTRY_KEYS.to_a
    end
  end


  def self.open_tokyocabinet(path, write, serializer = nil, tokyocabinet_class = TokyoCabinet::HDB)
    write = true unless File.exists? path

    FileUtils.mkdir_p File.dirname(path) unless File.exists?(File.dirname(path))

    database = Persist::TCAdapter.open(path, write, tokyocabinet_class)

    unless serializer == :clean
      TSV.setup database
      database.serializer = serializer || database.serializer
    end

    database
  end
end
