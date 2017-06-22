require 'rbbt/packed_index'

module Persist

  module PKIAdapter
    include Persist::TSVAdapter

    attr_accessor :pos_function

    def self.open(path, write, pattern, &pos_function)
      db = CONNECTIONS[path] ||= PackedIndex.new(path, write, pattern)
      db.extend Persist::PKIAdapter
      db.persistence_path = path
      db.pos_function = pos_function
      db
    end

    def persistence_path=(value)
      @persistence_path = value
      @file = value
    end

    def metadata_file
      @metadata_file ||= self.persistence_path + '.metadata'
    end

    def metadata
      return {} unless File.exist? metadata_file
      Open.open(metadata_file, :mode => "rb") do |f|
        Marshal.load(f)
      end
    end

    def set_metadata(k,v)
      metadata = self.metadata
      metadata[k] = v
      Misc.sensiblewrite(metadata_file, Marshal.dump(metadata))
    end

    def [](key, clean = false)
      if TSV::ENTRY_KEYS.include? key
        metadata[key]
      else
        key = pos_function.call(key) if pos_function and not clean
        res = super(key)
        res.extend MultipleResult unless res.nil?
        res
      end
    end

    def value(pos)
      self.send(:[], pos, true)
    end

    def []=(key, value)
      if TSV::ENTRY_KEYS.include? key
        set_metadata(key, value)
      else
        add key, value
      end
    end
     
    def add(key, value)
      key = pos_function.call(key) if pos_function 
      if Numeric === key
        @_last ||= -1
        skipped = key - @_last - 1
        skipped.times do
          self.send(:<<, nil)
        end
        @_last = key
      end
      self.send(:<<, value)
    end

    def add_range_point(key, value)
      key = pos_function.call(key) if pos_function
      super(key, value)
    end

    def include?(i)
      return true if Numeric === i and i < size
      return true if metadata.include? i
      false
    end

    def each
      size.times do |i|
        yield i, value(i)
      end
    end

    def keys
      []
    end
  end

  def self.open_pki(path, write, pattern, &pos_function)
    FileUtils.mkdir_p File.dirname(path) unless File.exist?(File.dirname(path))

    database = Persist::PKIAdapter.open(path, write, pattern, &pos_function)

    #TSV.setup database

    #database.serializer = :clean

    database
  end
end
