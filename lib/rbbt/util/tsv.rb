require 'tc_hash'
require 'digest'
require 'fileutils'

def add_defaults(options, defaults = {})
  new_options = options.dup
  defaults.each do |key, value|
    new_options[key] = value if new_options[key].nil?
  end
  new_options
end

class TSV
  class FieldNotFoundError < StandardError;end

  #{{{ Persistence

  PersistenceHash = TCHash

  CACHE_DIR="/tmp/tsv_persistent_cache"
  FileUtils.mkdir_p(CACHE_DIR) unless File.exist?(CACHE_DIR)
  def self.cachedir=(dir)
    @@cachedir=dir
    FileUtils.mkdir_p(dir) unless File.exist?(dir)
  end
  def self.cachedir
    @@cachedir ||= CACHE_DIR
  end

  def self.get_persistence_file(file, prefix, options = {})
    File.join(cachedir, prefix.gsub(/\s/,'_').gsub(/\//,'>') + Digest::MD5.hexdigest([file, options].inspect))
  end

  @debug = ENV['TSV_DEBUG'] == "true"
  def self.log(message)
    STDERR.puts message if @debug == true
  end

  def self.debug=(value)
    @debug = value
  end



  #{{{ Parsing
  
  def self.parse_fields(io, delimiter = "\t")
    sep_reg = Regexp === delimiter ? delimiter : Regexp.quote(delimiter)
    fields = io.split(/#{sep_reg}/, -1)
    fields
  end

  def self.field_pos(fields, field)
    case
    when Integer === field
      field
    else
      raise FieldNotFoundError, "TSV has no field information" if fields.nil?
      fields.each_with_index{|f,i| return i if f =~ /#{Regexp.quote(field)}/i}
      raise FieldNotFoundError, "Field #{ field } was not found"
    end
  end

  def self.zip_fields(list)
    return [] if list.nil? || list.empty?
    list[0].zip(*list[1..-1])
  end

  def self.each_line(file, &block)
    while !file.eof
      yield file.gets.chomp
    end
  end

  def self.parse(file, options = {})

    # Prepare options
    options = add_defaults options, 
      :sep              => "\t",
      :sep2             => "|",
      :native           => 0,
      :extra            => nil,
      :fix              => nil,
      :exclude          => nil,
      :select           => nil,
      :single           => false,
      :flatten          => false,
      :keep_empty       => false,
      :case_insensitive => false,
      :header_hash  => '#' ,
      :persistence_file => nil
    

    options[:extra]   = [options[:extra]] if options[:extra] != nil && ! (Array === options[:extra])
    options[:flatten] = true if options[:single]

    # Open data store
    data = options[:persistence_file].nil? ? {} : PersistenceHash.get(options[:persistence_file], true)

    header_fields = nil
    first_line    = true
    id_pos        = nil
    extra_pos     = nil
    Open.read(file){|line|

      if first_line
        first_line = false
        header_line = line =~ /^#{options[:header_hash]}/

        if header_line
          header_fields    = parse_fields(line, options[:sep])
          header_fields[0] = header_fields[0][(0 + options[:header_hash].length)..-1] # Remove initial hash character
        end

        id_pos = field_pos(header_fields, options[:native])

        if options[:extra].nil?
          parts = parse_fields(line, options[:sep])
          extra_pos = (0..(parts.length - 1 )).to_a
          extra_pos.delete(id_pos) 
        else
          extra_pos = options[:extra].collect{|pos| field_pos(header_fields, pos) }
        end

        next if header_line
      end

      # Get header file if present
      
      # Select and fix lines
      next if ! options[:exclude].nil? &&   options[:exclude].call(line)
      next if ! options[:select].nil?  && ! options[:select].call(line)
      line = options[:fix].call(line) if ! options[:fix].nil?

      ### Process line

      # Chunk fields
      parts = parse_fields(line, options[:sep])

      # Get id field
      next if parts[id_pos].nil? || parts[id_pos].empty?
      ids    = parse_fields(parts[id_pos], options[:sep2])
      ids.collect!{|id| id.downcase } if options[:case_insensitive]

      # Get extra fields
      extra = parts.values_at(*extra_pos)

      main_entry = ids.shift
      ids.each do |id|
        data[id] = "__Ref:#{main_entry}"
      end

      case
      when options[:single]
        data[main_entry] ||= extra.first
      when options[:flatten] && ! options[:single]
        data[main_entry] ||= []
        values = extra.collect{|value| parse_fields(value, options[:sep2])}.flatten.compact
        data[main_entry] = data[main_entry] + values
      else
        entry = data[main_entry] || []
        while entry =~ /__Ref:(.*)/ do
          entry = data[$1]
        end
        extra.each_with_index do |value, i|
          next if (value.nil? || value.empty?) && ! options[:keep_empty]
          fields = parse_fields(value, options[:sep2])
          fields = [""] if fields.empty?
          entry[i] ||= []
          entry[i].concat fields
        end
        data[main_entry] = entry
      end
    }

    # Save header information
    key_field = nil
    fields   = nil
    if header_fields && header_fields.any?
      key_field = header_fields[id_pos] 
      fields = header_fields.values_at(*extra_pos) 
    end

    data.read if PersistenceHash === data

    [data, key_field, fields]
  end

  attr_accessor :data, :key_field, :fields, :case_insensitive, :filename
  def initialize(file, options = {})
    @case_insensitive = options[:case_insensitive] == true

    case
    when Hash === file || PersistenceHash === file
      @filename = Hash
      @data = file
      return self
    when String === file && File.exists?(file)
      @filename = File.expand_path file
      file = File.open(file)
    when File === file
      @filename = File.expand_path file.path
    when String === file
      @filename = String
      file = StringIO.new(file)
    end

    if options[:persistence]
      options.delete :persistence
      persistence_file = TSV.get_persistence_file @filename, "file:#{ @filename }:", options

      if File.exists? persistence_file
        TSV.log "Loading Persistence for #{ @filename } in #{persistence_file}"
        @data      = PersistenceHash.get(persistence_file, false)
        @key_field = @data.key_field
        @fields    = @data.fields
      else
        TSV.log "Persistent Parsing for #{ @filename } in #{persistence_file}"
        @data, @key_field, @fields = TSV.parse(file, options.merge(:persistence_file => persistence_file))
        @data.key_field            = @key_field
        @data.fields               = @fields
        @data.read
      end
    else
      TSV.log "Non-persistent parsing for #{ @filename }"
      @data, @key_field, @fields = TSV.parse(file, options)
    end

    @case_insensitive = options[:case_insensitive] == true
  end

  def follow(value)
    if String === value && value =~ /__Ref:(.*)/
      return @data[$1]
    else
      return value
    end
  end

  def [](key)

    if Array === key
      return @data[key] if @data[key] != nil
      key.each{|k| v = self[k]; return v unless v.nil?}
      return nil
    end

    key = key.downcase if @case_insensitive
    follow @data[key]
  end

  def []=(key, value)
    key = key.downcase if @case_insensitive
    @data[key] = value
  end

  def keys
    @data.keys
  end

  def values
    @data.values
  end

  def values_at(*keys)
    keys.collect{|k|
      self[k]
    }
  end

  def each(&block)
    @data.each do |key, value|
      block.call(key, follow(value))
    end
  end

  def merge!(new_data)
    new_data.each do |key, value|
      self[key] = value
    end
  end

  def sort_by(&block)
    @data.sort_by &block
  end

  def sort(&block)
    @data.sort &block
  end

  def size
    @data.size
  end

  #{{{ Index
  def self.reorder(native, others, pos)
    return [native, others] if pos.nil? 

    [others[pos], others.unshift(native)]
  end


  def index(options = {})
    pos = nil
    if options[:field] && key_field !~ /#{Regexp.quote options[:field]}/i && ! fields.nil? && fields.any?
      pos = TSV.field_pos(fields, options[:field])
    end

    data = {}
    each do |key, values|
      key, values = self.class.reorder(key, values, pos) unless pos.nil?
      
      next if key.nil?

      if Array === key
        key.flatten!
        key.compact
      else
        key = [key]
      end

      values.flatten.compact.each do |value|
        value = value.downcase if options[:case_insensitive]
        data[value] ||= []  
        data[value].concat key
      end
    end

    if options[:persistence_file]
      if File.exists? options[:persistence_file]
        index = TSV.new(PersistenceHash.get(options[:persistence_file], false), :case_insensitive => options[:case_insensitive])
      else
        index = TSV.new(PersistenceHash.get(options[:persistence_file], false), :case_insensitive => options[:case_insensitive])
        index.merge! data
      end
    else
      index = TSV.new(data, :case_insensitive => options[:case_insensitive])
    end

    if ! pos.nil?
      index.key_field = fields[pos] 
    else
      index.key_field = key_field
    end


    index
  end

  def slice(*fields)
    new = TSV.new({}, :case_insensitive => @case_insensitive)
    positions = fields.collect{|field| TSV.field_pos(self.fields, field)}
    data.each do |key, values|
      new[key] = values.values_at(*positions)
    end
    new.fields = fields
    new.key_field = self.key_field

    new
  end

  #{{{ Helpers

  def self.index(file, options = {})
    opt_data = options.dup
    opt_index = options.dup
    opt_data.delete  :field
    opt_data.delete  :persistence
    opt_index.delete :persistence

    opt_data[:persistence] = true if options[:data_persistence]

    opt_index.merge! :persistence_file => get_persistence_file(file, "index:#{ file }_#{options[:field]}:", opt_index) if options[:persistence]

    if ! opt_index[:persistence_file].nil? && File.exists?(opt_index[:persistence_file])
      TSV.log "Reloading persistent index for #{ file }: #{opt_index[:persistence_file]}"
      TSV.new(PersistenceHash.get(opt_index[:persistence_file], false),opt_index)
    else
      TSV.log "Creating index for #{ file }: #{opt_index[:persistence_file]}"
      data = TSV.new(file, opt_data)
      data.index(opt_index)
    end
  end
end


if __FILE__ == $0

 i = TSV.index('/home/mvazquezg/rbbt/data/organisms/Hsa/identifiers2', :format => "Ensembl Protein ID", :persistence => true)
end
