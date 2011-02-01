require 'tokyocabinet'

class TCHash < TokyoCabinet::HDB
  class OpenError < StandardError;end
  class KeyFormatError < StandardError;end

  Serializer = Marshal

  CONNECTIONS = {}

  FIELD_INFO_ENTRIES = {
    :fields    => '__tokyocabinet_hash_fields', 
    :key_field => '__tokyocabinet_hash_key_field',
    :filename  => '__tokyocabinet_hash_filename',
    :type      => '__tokyocabinet_hash_type',
    :namespace      => '__tokyocabinet_hash_namespace',
    :case_insensitive      => '__tokyocabinet_hash_case_insensitive'
  }

  FIELD_INFO_ENTRIES.each do |entry, key|
    class_eval do 
      define_method entry.to_s, proc{self[key]}
      define_method entry.to_s + "=", proc{|value| write unless write?; self[key] = value}
    end
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

  #{{{ ACESSORS 

  alias original_get_brackets []
  def [](key)
    return nil unless String === key
    result = self.original_get_brackets(key)
    result ? Serializer.load(result) : nil
  end

  alias original_set_brackets []=
  def []=(key,value)
    raise KeyFormatError, "Key must be a String, its #{key.class.to_s}" unless String === key
    raise "Closed TCHash connection" unless write?
    self.original_set_brackets(key, Serializer.dump(value))
  end

  def values_at(*args)
    args.collect do |key|
      self[key]
    end
  end

  alias original_keys keys
  def keys
    list = self.original_keys
    indexes = FIELD_INFO_ENTRIES.values.collect do |field| list.index(field) end.compact.sort.reverse
    indexes.each do |index| list.delete_at index end
    list
  end

  alias original_values values
  def values
    values = self.original_values
    keys   = self.original_keys
    indexes = FIELD_INFO_ENTRIES.values.collect do |field| keys.index(field) end.compact.sort.reverse
    indexes.each do |index| values.delete_at index end

    values.collect{|v| Serializer.load(v)}
  end

  # This version of each fixes a problem in ruby 1.9. It also
  # removes the special entries
  def each(&block)
    values = self.original_values.collect{|v| Serializer.load v}
    keys   = self.original_keys
    indexes = FIELD_INFO_ENTRIES.values.collect do |field| keys.index(field) end.compact.sort.reverse
    indexes.sort.reverse.each do |index| values.delete_at(index); keys.delete_at(index) end

    keys.zip(values).each &block
  end

  alias original_each each
  
  def collect
    res = []
    self.each{|k, v| res << yield(k,v)}
    res
  end

  def merge!(data)
    new_data = {}
    data.each do |key, values|
      self[key] = values
    end
  end

end
