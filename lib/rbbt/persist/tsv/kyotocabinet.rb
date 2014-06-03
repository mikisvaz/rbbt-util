require 'kyotocabinet'

module Persist

  module KCAdapter
    include Persist::TSVAdapter
    attr_accessor :kyotocabinet_class

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
      database.persistence_path ||= real_path

      database
    end

    def close
      @closed = true
      super
      self
    end

    def read(force = false)
      return if not write? and not closed and not force
      self.close
      if !self.open(@persistence_path, KyotoCabinet::DB::OREADER)
        raise "Open error #{ res }. Trying to open file #{@persistence_path}"
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
