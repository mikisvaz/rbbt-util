require 'tokyocabinet'

module Persist

  module TCAdapter
    include Persist::TSVAdapter

    attr_accessor :tokyocabinet_class

    def self.open(path, write, serializer, tokyocabinet_class = TokyoCabinet::HDB)
      tokyocabinet_class = TokyoCabinet::HDB if tokyocabinet_class == "HDB" or tokyocabinet_class.nil?
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
  end

  def self.open_tokyocabinet(path, write, serializer = nil, tokyocabinet_class = TokyoCabinet::HDB)
    write = true unless File.exists? path

    FileUtils.mkdir_p File.dirname(path) unless File.exists?(File.dirname(path))

    database = Persist::TCAdapter.open(path, write, serializer, tokyocabinet_class)

    unless serializer == :clean
      TSV.setup database
      database.serializer = serializer || database.serializer
    end

    database
  end
end
