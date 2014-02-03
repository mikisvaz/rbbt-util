require 'kyotocabinet'

module Persist

  module KCAdapter
    attr_accessor :persistence_path, :kyotocabinet_class, :closed, :writable

    def self.open(path, write, kyotocabinet_class = "kch")
      real_path = path + ".#{kyotocabinet_class}"

      @persistence_path = real_path

      flags = (write ? KyotoCabinet::DB::OWRITER | KyotoCabinet::DB::OCREATE : nil)
      database = 
        CONNECTIONS[path] ||= begin
                                db = KyotoCabinet::DB.new
                                db.open(real_path, flags)
                                db
                              end

      database.extend KCAdapter
      database.persistence_path ||= path

      database
    end

    def keys
      keys = []
      each_key{|k| keys << k}
      keys
    end

    def prefix(key)
      range(key, 1, key + 255.chr, 1)
    end

    def get_prefix(key)
      keys = prefix(key)
      select(:key => keys)
    end

    def include?(key)
      value = get(key)
      ! value.nil?
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
      if !self.open(@persistence_path, KyotoCabinet::DB::OREADER)
        raise "Open error. Trying to open file #{@persistence_path}"
      end
      @writable = false
      @closed = false
      self
    end

    def write(force = true)
      return if write? and not closed and not force
      self.close

      if !self.open(@persistence_path, KyotoCabinet::DB::OWRITER)
        raise "Open error. Trying to open file #{@persistence_path}"
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
      super(*args) - TSV::ENTRY_KEYS.to_a
    end
  end


  def self.open_kyotocabinet(path, write, serializer = nil,  kyotocabinet_class= 'kch')
    write = true unless File.exists? path

    FileUtils.mkdir_p File.dirname(path) unless File.exists?(File.dirname(path))

    database = Persist::KCAdapter.open(path, write, kyotocabinet_class)

    unless serializer == :clean
      TSV.setup database
      database.serializer = serializer || database.serializer
    end

    database
  end
end
