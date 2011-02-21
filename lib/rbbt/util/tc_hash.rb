require 'rbbt/util/misc'
require 'tokyocabinet'

class TCHash < TokyoCabinet::HDB
  class OpenError < StandardError;end
  class KeyFormatError < StandardError;end

  class StringSerializer
    def self.dump(str); str.to_s; end
    def self.load(str); str; end
  end

  class StringArraySerializer
    def self.dump(array)
      array.collect{|a| a.to_s} * "\t"
    end

    def self.load(string)
      string.split(/\t/)
    end
  end

  class StringDoubleArraySerializer
    def self.dump(array)
      array.collect{|a| a.collect{|a| a.to_s} * "|"} * "\t"
    end

    def self.load(string)
      string.split(/\t/).collect{|l| l.split("|")}
    end
  end


  ALIAS = {:marshal => Marshal, nil => Marshal, :single => StringSerializer, :list => StringArraySerializer, :double => StringDoubleArraySerializer}

  CONNECTIONS = {}

  FIELD_INFO_ENTRIES = {
    :type             => '__tokyocabinet_hash_type',
    :serializer       => '__tokyocabinet_hash_serializer',
    :identifiers      => '__tokyocabinet_hash_identifiers',
    :datadir          => '__tokyocabinet_hash_datadir',
    :fields           => '__tokyocabinet_hash_fields',
    :key_field        => '__tokyocabinet_hash_key_field',
    :filename         => '__tokyocabinet_hash_filename',
    :namespace        => '__tokyocabinet_hash_namspace',
    :type             => '__tokyocabinet_hash_type',
    :case_insensitive => '__tokyocabinet_hash_case_insensitive'
  }

  FIELD_INFO_ENTRIES.each do |entry, key|
    class_eval do 
      define_method entry.to_s, proc{v = self.original_get_brackets(key); v.nil? ? nil : Marshal.load(v)}
      define_method entry.to_s + "=", proc{|value| write unless write?; self.original_set_brackets key, Marshal.dump(value)}
    end
  end

  def serializer
    @serializer
  end

  def serializer=(value)
    self.original_set_brackets(FIELD_INFO_ENTRIES[:serializer],value) unless value.nil?
  end

  alias original_open open
  def open(write = false)
    flags = write ? TokyoCabinet::HDB::OWRITER | TokyoCabinet::HDB::OCREAT : TokyoCabinet::BDB::OREADER
    if !self.original_open(@path_to_db, flags)
      ecode = self.ecode
      raise OpenError, "Open error: #{self.errmsg(ecode)}. Trying to open file #{@path_to_db}"
    end

    @write = write

    if write
      self.original_set_brackets(FIELD_INFO_ENTRIES[:serializer], @serializer.to_s) unless @serializer.nil?
    else
      serializer_str = self.original_get_brackets(FIELD_INFO_ENTRIES[:serializer])

      if serializer_str.nil? or serializer_str.empty? 
        @serializer = Marshal
      else
        mod = Misc.string2const serializer_str
       @serializer = mod
      end
    end
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

  def initialize(path, write = false, serializer = Marshal)
    super()

    serializer = ALIAS[serializer] if ALIAS.include? serializer

    @path_to_db = path
    @serializer = serializer

    if write || ! File.exists?(@path_to_db)
      self.open(true)
    else
      self.open(false)
    end
  end

  def self.get(path, write = false, serializer = Marshal)
    serializer = ALIAS[serializer] if ALIAS.include? serializer
    @serializer = serializer
    d = CONNECTIONS[path] ||= self.new(path, false, @serializer)
    write ? d.write : d.read
    d
  end

  #{{{ ACESSORS 

  alias original_get_brackets []
  def [](key)
    return nil unless String === key
    result = self.original_get_brackets(key)
    result ? @serializer.load(result) : nil
  end

  alias original_set_brackets []=
  def []=(key,value)
    raise KeyFormatError, "Key must be a String, its #{key.class.to_s}" unless String === key
    raise "Closed TCHash connection" unless write?
    self.original_set_brackets(key, serializer.dump(value))
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

    values.collect{|v| serializer.load(v)}
  end

  # This version of each fixes a problem in ruby 1.9. It also
  # removes the special entries
  def each(&block)
    values = self.original_values
    keys   = self.original_keys
    indexes = FIELD_INFO_ENTRIES.values.collect do |field| keys.index(field) end.compact.sort.reverse
    indexes.sort.reverse.each do |index| values.delete_at(index); keys.delete_at(index) end

    keys.zip(values.collect{|v| serializer.load v}).each &block
  end

  alias original_each each
  
  def collect
    res = []
    self.each{|k, v| 
      if block_given?
        res << yield(k,v)
      else
        res << [k,v]
      end
    }
    res
  end

  def merge!(data)
    raise "Closed TCHash connection" unless write?
    serialized = 
      data.collect{|key, values| [key.to_s, serializer.dump(values)]}
    if tranbegin
      serialized.each do |key, values|
        self.putasync(key, values)
      end
      trancommit
    else
      raise "Transaction cannot initiate"
    end
  end

end
