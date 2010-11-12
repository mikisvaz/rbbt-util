require 'tokyocabinet'

class TCHash < TokyoCabinet::HDB
  class OpenError < StandardError;end
  class KeyFormatError < StandardError;end

  Serializer = Marshal

  FIELD_INFO_ENTRIES = {:fields => '__tokyocabinet_hash_fields', :key_field => '__tokyocabinet_hash_native_field'}
  CONNECTIONS = {}

  FIELD_INFO_ENTRIES.each do |entry, key|
    class_eval do 
      define_method entry.to_s, proc{self[key]}
      define_method entry.to_s + "=", proc{|value| write unless write?; self[key] = value}
    end
  end

  alias original_get_brackets []
  def [](key)
    return nil unless String === key
    result = self.original_get_brackets(key)
    result ? Serializer.load(result) : nil
  end

  alias original_set_brackets []=
  def []=(key,value)
    raise KeyFormatError, "Key must be a String, its #{key.class.to_s}" unless String === key
    write unless write?
    self.original_set_brackets(key, Serializer.dump(value))
  end

  def values_at(*args)
    puts "Finding #{args.inspect}"
    args.collect do |key|
      self[key]
    end
  end

  alias original_values values
  def values
    self.values.collect{|v| Serializer.load v}
  end

  alias original_keys keys
  def keys
    list = self.original_keys
    FIELD_INFO_ENTRIES.values do |field| list.delete field  end
    list
  end

  def merge!(data)
    new_data = {}
    data.each do |key, values|
      self[key] = values
    end
  end

  alias original_each each
  def each
    self.original_each {|k, v| yield(k, Serializer.load(v)) unless FIELD_INFO_ENTRIES.values.include? k }
  end


  alias original_open open
  def open(write = false)
    flags = write ? TokyoCabinet::HDB::OWRITER | TokyoCabinet::HDB::OCREAT : TokyoCabinet::BDB::OREADER
    if !self.original_open(@path_to_db, flags)
      ecode = self.ecode
      raise OpenError, "Open error: #{self.errmsg(ecode)}. Trying to open file #{@path_to_db}"
    end
    @write = write
  end

  def write?
    @write
  end

  def write
    self.close
    self.open(true)
  end

  def read
    self.close
    self.open(false)
  end

  def initialize(path, write = false)
    super()
    @path_to_db = path

    if write || ! File.exists?(@path_to_db)
      self.open(true)
    else
      self.open(false)
    end
  end

  def self.get(path, write = false)
    d = CONNECTIONS[path] ||= self.new(path, false)
    write ? d.write : d.read
    d
  end
end
