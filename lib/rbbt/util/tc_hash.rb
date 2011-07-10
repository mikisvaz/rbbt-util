require 'rbbt/util/misc'
require 'tokyocabinet'
require 'set'

class TCHash < TokyoCabinet::HDB
  class OpenError < StandardError;end
  class KeyFormatError < StandardError;end

  class IntegerSerializer
    def self.dump(i); [i].pack("l"); end
    def self.load(str); str.unpack("l").first; end
  end

  class FloatSerializer
    def self.dump(i); [i].pack("d"); end
    def self.load(str); str.unpack("d").first; end
  end

  class IntegerArraySerializer
    def self.dump(a); a.pack("l*"); end
    def self.load(str); str.unpack("l*"); end
  end

  class StringSerializer
    def self.dump(str); str.to_s; end
    def self.load(str); str; end
  end

  class StringArraySerializer
    def self.dump(array)
      array.collect{|a| a.to_s} * "\t"
    end

    def self.load(string)
      string.split("\t", -1)
    end
  end

  class StringDoubleArraySerializer
    def self.dump(array)
      array.collect{|a| a.collect{|a| a.to_s} * "|"} * "\t"
    end

    def self.load(string)
      string.split("\t", -1).collect{|l| l.split("|", -1)}
    end
  end

  class TSVSerializer
    def self.dump(tsv)
      tsv.to_s
    end

    def self.load(string)
      TSV.new StringIO.new(string)
    end
  end



  ALIAS = {
    :integer => IntegerSerializer, 
    :float => FloatSerializer, 
    :integer_array => IntegerArraySerializer,
    :marshal => Marshal,
    :single => StringSerializer,
    :string => StringSerializer,
    :list => StringArraySerializer,
    :double => StringDoubleArraySerializer,
    :tsv => TSVSerializer
  }

  CONNECTIONS = {}

  FIELD_INFO_ENTRIES = {
    :type             => '__tokyocabinet_hash_type',
    :serializer       => '__tokyocabinet_hash_serializer',
    :identifiers      => '__tokyocabinet_hash_identifiers',
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

  def size
    keys.length
  end

  def delete(key)
    raise "Cannot deleted key: closed connection" if not write?
    out(key) or raise "Not deleted"
  end

  alias original_include? include?
  def include?(key)
    return nil unless String === key
    original_include? key
  end

  attr_accessor :serializer, :path_to_db
  def serializer=(serializer)
    
    if ALIAS.include? serializer.to_sym
      @serializer = ALIAS[serializer.to_sym]
    else
      @serializer = serializer
    end
    self.original_set_brackets(FIELD_INFO_ENTRIES[:serializer], @serializer.to_s)
  end

  alias original_open open
  def open(write = false, serializer = nil)
    flags = (write ? TokyoCabinet::HDB::OWRITER | TokyoCabinet::HDB::OCREAT : TokyoCabinet::BDB::OREADER)

    FileUtils.mkdir_p File.dirname(@path_to_db) unless File.exists?(File.dirname(@path_to_db))
    if !self.original_open(@path_to_db, flags)
      ecode = self.ecode
      raise OpenError, "Open error: #{self.errmsg(ecode)}. Trying to open file #{@path_to_db}"
    end

    @write = write

    if @serializer.nil?

      if self.include? FIELD_INFO_ENTRIES[:serializer]
        serializer_str = self.original_get_brackets(FIELD_INFO_ENTRIES[:serializer])

        mod = Misc.string2const serializer_str
        @serializer = mod

      else
        raise "No serializer specified" if (serializer || @serializer).nil?

        self.original_set_brackets(FIELD_INFO_ENTRIES[:serializer], serializer.to_s) unless self.include? FIELD_INFO_ENTRIES[:serializer]
        @serializer = serializer
      end
    end
  end

  def write?
    @write
  end

  def write
    self.sync
    self.close
    self.open(true)
  end

  def read
    self.sync
    self.close
    self.open(false)
  end

  def initialize(path, write = false, serializer = nil)
    super()

    if ALIAS.include? serializer
      @serializer = ALIAS[serializer]
    else
      @serializer = serializer
    end

    @path_to_db = path

    if write || ! File.exists?(@path_to_db)
      @serializer = Marshal if @serializer.nil?
      self.setcache(100000) or raise "Error setting cache"
      self.open(true, @serializer)
      self.original_set_brackets(FIELD_INFO_ENTRIES[:serializer], @serializer.to_s)
    else
      self.open(false)
    end
  end

  def self.get(path, write = false, serializer = nil)
    if not (TrueClass === write or FalseClass === write) and serializer.nil?
      serializer = write
      write = false
    end

    if ALIAS.include? serializer
      serializer = ALIAS[serializer] 
    else
      serializer = serializer
    end

    case
    when !File.exists?(path)
      CONNECTIONS[path] = self.new(path, true, serializer)
    when (not CONNECTIONS.include?(path))
      CONNECTIONS[path] = self.new(path, false, serializer)
    end

    d = CONNECTIONS[path] 
    
    if write 
      d.write unless d.write?
    else
      d.read if d.write?
    end

    d
  end

  #{{{ ACESSORS 

  alias original_get_brackets []
  def [](key)
    return nil unless String === key
    result = self.original_get_brackets(key)
    if result.nil? or (String === result and result =~ /__Ref:/) 
      result 
    else
      @serializer.load(result)
    end
  end

  alias original_set_brackets []=
  def []=(key,value)
    raise KeyFormatError, "Key must be a String, its #{key.class.to_s}" unless String === key
    raise "Closed TCHash connection" unless write?
    if String === value and value =~ /^__Ref/
      self.original_set_brackets(key, value)
    else
      self.original_set_brackets(key, serializer.dump(value))
    end
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

  alias real_original_each each
  # This version of each fixes a problem in ruby 1.9. It also
  # removes the special entries
  def each(&block)
    values = self.original_values
    keys   = self.original_keys
    indexes = FIELD_INFO_ENTRIES.values.collect do |field| keys.index(field) end.compact.sort.reverse
    indexes.sort.reverse.each do |index| values.delete_at(index); keys.delete_at(index) end

    keys.zip(values.collect{|v| serializer.load v}).each &block
  end

  def each(&block)
    skippable = Set.new(FIELD_INFO_ENTRIES.values)
    real_original_each do |key, value|
      block.call(key, serializer.load(value)) unless skippable.include? key
    end
  end

  alias original_each each
  
  def collect(&block)
    skippable = Set.new(FIELD_INFO_ENTRIES.values)
    res = []
    real_original_each do |key,value|
      next if skippable.include? key
      if block_given?
        block.call(key, serializer.load(value)) 
      else
        res << [key, value]
      end
    end
    res
  end

  def merge!(data)
    raise "Closed TCHash connection" unless write?
    serialized = 
      data.collect{|key, values| [key.to_s, serializer.dump(values)] }
    if tranbegin
      serialized.each do |key, values|
        self.putasync(key, values)
      end
      trancommit
    else
      raise "Transaction cannot initiate"
    end
  end
  
  def clear
    special_values = FIELD_INFO_ENTRIES.values.sort.collect{|k|  self.original_get_brackets(k)}
    restore = ! write?
    write if restore
    vanish
    FIELD_INFO_ENTRIES.values.sort.zip(special_values).each{|k,v|
      self.original_set_brackets(k,v) unless v.nil?
    }
    read if restore
  end

  def self.importtsv(file, path, options = {})
    CMD.cmd("tchmgr importtsv '#{ path }' #{ file }")
    f = Open.open(file)
    key_field, fields, header_options = TSV.parse_header(f)
    f.close
    options = header_options.merge! options

    db = TCHash.get(path, true, options[:type] || :double)
    db.key_field = key_field
    db.fields = fields
    %w(case_insensitive namespace identifiers fields key_field type filename cast).each do |key| 
      if options.include? key.to_sym
        if db.respond_to? "#{key}=".to_sym
          db.send("#{key}=".to_sym, options[key.to_sym])
        end
      end
    end 
    db.read

    db
  end

end
