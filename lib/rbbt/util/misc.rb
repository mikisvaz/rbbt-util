require 'lockfile'
require 'rbbt/util/chain_methods'
require 'rbbt/resource/path'
require 'rbbt/annotations'

module Misc
  class FieldNotFoundError < StandardError;end

  def self.counts(array)
    counts = Hash.new 0
    array.each do |e|
      counts[e] += 1
    end
    counts
  end
  
  IUPAC2BASE = {
    "A" => ["A"],
    "C" => ["C"],
    "G" => ["G"],
    "T" => ["T"],
    "U" => ["U"],
    "R" => "A or G".split(" or "),
    "Y" => "C or T".split(" or "),
    "S" => "G or C".split(" or "),
    "W" => "A or T".split(" or "),
    "K" => "G or T".split(" or "),
    "M" => "A or C".split(" or "),
    "B" => "C or G or T".split(" or "),
    "D" => "A or G or T".split(" or "),
    "H" => "A or C or T".split(" or "),
    "V" => "A or C or G".split(" or "),
    "N" => %w(A C T G),
  }

  BASE2COMPLEMENT = {
    "A" => "T",
    "C" => "G",
    "G" => "C",
    "T" => "A",
    "U" => "A",
  }

  def self.IUPAC_to_base(iupac)
    IUPAC2BASE[iupac]
  end

  def self.is_filename?(string)
    return true if Path === string
    return true if String === string and string.length < 265 and File.exists? string
    return false
  end

  def self.intersect_sorted_arrays(a1, a2)
    e1, e2 = a1.shift, a2.shift
    intersect = []
    while true
      case
      when (e1 and e2)
        case e1 <=> e2
        when 0
          intersect << e1
          e1, e2 = a1.shift, a2.shift
        when -1
          e1 = a1.shift
        when 1
          e2 = a2.shift
        end
      else
        break
      end
    end
    intersect
  end

  def self.merge_sorted_arrays(a1, a2)
    e1, e2 = a1.shift, a2.shift
    new = []
    while true
      case
      when (e1 and e2)
        case e1 <=> e2
        when 0
          new << e1 
          e1, e2 = a1.shift, a2.shift
        when -1
          new << e1
          e1 = a1.shift
        when 1
          new << e2
          e2 = a2.shift
        end
      when e2
        new << e2
        new.concat a2
        break
      when e1
        new << e1
        new.concat a1
        break
      else
        break
      end
    end
    new
  end

  def self.array2hash(array)
    hash = {}
    array.each do |key, value|
      hash[key] = value
    end
    hash
  end

  def self.zip2hash(list1, list2)
    array2hash(list1.zip(list2))
  end

  def self.process_to_hash(list)
    result = yield list
    zip2hash(list, result)
  end

  def self.env_add(var, value, sep = ":", prepend = true)
    ENV[var] ||= ""
    return if ENV[var] =~ /(#{sep}|^)#{Regexp.quote value}(#{sep}|$)/
      if prepend
        ENV[var] = value + sep + ENV[var]
      else
        ENV[var] += sep + ENV[var]
      end
  end

  def self.benchmark(repeats = 1)
    require 'benchmark'
    res = nil
    begin
      measure = Benchmark.measure do
        repeats.times do
          res = yield
        end
      end
      puts "Benchmark for #{ repeats } repeats"
      puts measure
    rescue Exception
      puts "Benchmark aborted"
      raise $!
    end
    res
  end

  def self.profile
    require 'ruby-prof'
    RubyProf.start
    begin
      res = yield
    rescue Exception
      puts "Profiling aborted"
      raise $!
    ensure
      result = RubyProf.stop
      printer = RubyProf::FlatPrinter.new(result)
      printer.print(STDOUT, 0)
    end

    res
  end

  def self.insist(times = 3)
    try = 0
    begin
      yield
    rescue
      try += 1
      retry if try < times
    end
  end

  def self.try3times(&block)
    insist(3, &block)
  end

  def self.hash2string(hash)
    hash.sort_by{|k,v| k.to_s}.collect{|k,v| 
      next unless %w(Symbol String Float Fixnum Integer TrueClass FalseClass Module Class Object).include? v.class.to_s
      [ Symbol === k ? ":" << k.to_s : k,
        Symbol === v ? ":" << v.to_s : v] * "="
    }.compact * "#"
  end

  def self.path_relative_to(basedir, path)
    path = File.expand_path(path)
    basedir = File.expand_path(basedir)

    if path =~ /#{Regexp.quote basedir}\/(.*)/
      return $1
    else
      return nil
    end
  end

  def self.lock(file, *args)
    FileUtils.mkdir_p File.dirname(File.expand_path(file)) unless File.exists?  File.dirname(File.expand_path(file))
    lockfile = Lockfile.new(file + '.lock')
    lockfile.lock do
      yield file, *args
    end
  end

  def self.common_path(dir, file)
    file = File.expand_path file
    dir = File.expand_path dir

    return true if file == dir
    while File.dirname(file) != file
      file = File.dirname(file)
      return true if file == dir
    end

    return false
  end

  def self.in_dir(dir)
    old_pwd = FileUtils.pwd
    res = nil
    begin
      FileUtils.mkdir_p dir unless File.exists? dir
      FileUtils.cd dir
      res = yield
    ensure
      FileUtils.cd old_pwd
    end
    res
  end

  def self.fixutf8(string)
    if string.respond_to?(:valid_encoding?) and ! string.valid_encoding?
      @@ic ||= Iconv.new('UTF-8//IGNORE', 'UTF-8')
      @@ic.iconv(string)
    else
      string
    end
  end

  def self.sensiblewrite(path, content)
    begin
      case
      when String === content
        File.open(path, 'w') do |f|  f.write content  end
      when (IO === content or StringIO === content)
        File.open(path, 'w') do |f|  while l = content.gets; f.write l; end  end
      else
        File.open(path, 'w') do |f|  end
      end
    rescue Interrupt
      FileUtils.rm_f path
      raise "Interrupted (Ctrl-c)"
    rescue Exception
      FileUtils.rm_f path
      raise $!
    end
  end

  def self.add_defaults(options, defaults = {})
    case
    when Hash === options
      new_options = options.dup
    when String === options
      new_options = string2hash options
    else
      raise "Format of '#{options.inspect}' not understood. It should be a hash"
    end
    defaults.each do |key, value|
      next unless new_options[key].nil?

      new_options[key] = value 
    end
    new_options
  end

  def self.digest(text)
    Digest::MD5.hexdigest(text)
  end

  def self.hash2md5(hash)
    o = {}
    hash.keys.sort_by{|k| k.to_s}.each do |k|
      next if k == :monitor or k == "monitor" or k == :in_situ_persistence or k == "in_situ_persistence"
      v = hash[k]
      case
      when v.inspect =~ /:0x0/
        o[k] = v.inspect.sub(/:0x[a-f0-9]+@/,'')
        #when Resource::Path === v
        #  o[k] = "" << String.new(v.to_s)
      else
        o[k] = v
      end
    end

    if o.empty?
      ""
    else
      Digest::MD5.hexdigest(o.sort_by{|k| k.to_s}.inspect)
    end
  end

  def self.process_options(hash, *keys)
    if keys.length == 1
      hash.delete keys.first.to_sym
    else
      keys.collect do |key| hash.delete(key.to_sym) || hash.delete(key.to_s) end
    end
  end

  def self.pull_keys(hash, prefix)
    new = {}
    hash.keys.each do |key|
      if key.to_s =~ /#{ prefix }_(.*)/
        case
        when String === key
          new[$1] = hash.delete key
        when Symbol === key
          new[$1.to_sym] = hash.delete key
        end
      else
        if key.to_s == prefix.to_s
          new[key] = hash.delete key
        end
      end
    end

    new
  end

  def self.string2const(string)
    return nil if string.nil?
    mod = Kernel

    string.to_s.split('::').each do |str|
      mod = mod.const_get str
    end

    mod
  end

  def self.string2hash(string)

    options = {}
    string.split(/#/).each do |str|
      if str.match(/(.*)=(.*)/)
        option, value = $1, $2
      else
        option, value = str, true
      end

    option = option.sub(":",'').to_sym if option.chars.first == ':'
    value  = value.sub(":",'').to_sym if String === value and value.chars.first == ':'

    if value == true
      options[option] = option.to_s.chars.first != '!' 
    else
      options[option] = Thread.start do
        $SAFE = 0;
        case 
        when value =~ /^(?:true|T)$/i
          true
        when value =~ /^(?:false|F)$/i
          false
        when Symbol === value
          value
        when (String === value and value =~ /^\/(.*)\/$/)
          Regexp.new /#{$1}/
        else
          begin
            Kernel.const_get value
          rescue
            begin  
              raise if value =~ /[a-z]/ and defined? value
              eval(value) 
            rescue Exception
              value 
            end
          end
        end
      end.value
    end
    end

    options
  end

  def self.field_position(fields, field, quiet = false)
    return field if Integer === field or Range === field
    raise FieldNotFoundError, "Field information missing" if fields.nil? && ! quiet
    fields.each_with_index{|f,i| return i if f == field}
    field_re = Regexp.new /#{field}/i
    fields.each_with_index{|f,i| return i if f =~ field_re}
    raise FieldNotFoundError, "Field #{ field.inspect } was not found" unless quiet
  end

  # Divides the array into +num+ chunks of the same size by placing one
  # element in each chunk iteratively.
  def self.divide(array, num)
    chunks = []
    num.times do chunks << [] end
    array.each_with_index{|e, i|
      c = i % num
      chunks[c] << e
    }
    chunks
  end

  def self.zip_fields(array)
    array[0].zip(*array[1..-1])
  end

end

module NamedArray
  extend ChainMethods
  self.chain_prefix = :named_array
  attr_accessor :fields

  def self.setup(array, fields)
    array.extend NamedArray
    array.fields = fields
    array
  end

  def merge(array)
    double = Array === array.first 
    new = self.dup
    (0..length - 1).each do |i|
      if double
        new[i] = new[i] + array[i]
      else
        new[i] << array[i]
      end
    end
    new
  end

  def positions(fields)
    if Array ==  fields
      fields.collect{|field|
        Misc.field_position(@fields, field)
      }
    else
      Misc.field_position(@fields, fields)
    end
  end

  def named_array_get_brackets(key)
    if defined? Entity
      entity = (defined?(Entity) and Entity.respond_to?(:formats)) ? Entity.formats[key] : nil
      if entity
        if entity.annotations.first == :format
          entity.setup(named_array_clean_get_brackets(Misc.field_position(fields, key)), key)
        else
          entity.setup(named_array_clean_get_brackets(Misc.field_position(fields, key)))
        end
      else
        named_array_clean_get_brackets(Misc.field_position(fields, key))
      end
    else
      named_array_clean_get_brackets(Misc.field_position(fields, key))
    end
  end

  def named_array_each(&block)
    if defined?(Entity) and not fields.nil? and not fields.empty?
      fields.zip(self) do |field,elem|
        entity = (defined?(Entity) and Entity.respond_to?(:formats)) ? Entity.formats[field] : nil
        if entity
          elem = elem.dup if elem.frozen?
          if entity.annotations.first == :format
            elem = entity.setup(elem, field) 
          else
            elem = entity.setup(elem)
          end
        else
        end
        yield(elem)
        elem
      end
    else
      named_array_clean_each &block
    end
  end

  def named_array_set_brackets(key,value)
    named_array_clean_set_brackets(Misc.field_position(fields, key), value)
  end

  def named_array_values_at(*keys)
    keys = keys.collect{|k| Misc.field_position(fields, k) }
    named_array_clean_values_at(*keys)
  end

  def zip_fields
    return [] if self.empty?
    zipped = Misc.zip_fields(self)
    zipped = zipped.collect{|v| NamedArray.setup(v, fields)}
    zipped 
  end

  def detach(file)
    file_fields = file.fields.collect{|field| field.fullname}
    detached_fields = []
    self.fields.each_with_index{|field,i| detached_fields << i if file_fields.include? field.fullname}
    fields = self.fields.values_at *detached_fields
    values = self.values_at *detached_fields
    values = NamedArray.name(values, fields)
    values.zip_fields
  end

  def report
    fields.zip(self).collect do |field,value|
      "\nAttributes:\n* #{ field }: #{ Array === value ? value * "|" : value }"
    end * "\n"
  end

end

class RBBTError < StandardError
  attr_accessor :info

  alias old_to_s to_s
  def to_s
    str = old_to_s.dup
    if info
      str << "\n" << "Additional Info:\n---\n" << info << "---"
    end
    str
  end
end

module IndiferentHash
  extend ChainMethods
  self.chain_prefix = :indiferent

  def indiferent_get_brackets(key)
    case 
    when (Symbol === key and indiferent_clean_include? key)
      indiferent_clean_get_brackets(key)
    when (Symbol === key and indiferent_clean_include? key.to_s)
      indiferent_clean_get_brackets(key.to_s)
    when (String === key and indiferent_clean_include? key)
      indiferent_clean_get_brackets(key)
    when (String === key and indiferent_clean_include? key.to_sym)
      indiferent_clean_get_brackets(key.to_sym)
    else
      indiferent_clean_get_brackets(key) 
    end
  end

  def indiferent_values_at(*key_list)
    res = []
    key_list.each{|key| res << indiferent_get_brackets(key)}
    res
  end

  def indiferent_include?(key)
    case
    when Symbol === key
      indiferent_clean_include?(key) or indiferent_clean_include?(key.to_s) 
    when String === key
      indiferent_clean_include?(key) or indiferent_clean_include?(key.to_sym) 
    else
      indiferent_clean_include?(key)
    end
  end

  def indiferent_delete(value)
    if indiferent_clean_include? value.to_s
      indiferent_clean_delete(value.to_s) 
    else
      indiferent_clean_delete(value.to_sym) 
    end
  end

  def self.setup(hash)
    return hash if IndiferentHash === hash
    hash.extend IndiferentHash
    hash
  end
end

module PDF2Text
  def self.pdftotext(filename)
    require 'rbbt/util/cmd'
    require 'rbbt/util/tmpfile'
    require 'rbbt/util/open'


    TmpFile.with_file(Open.open(filename, :nocache => true).read) do |pdf_file|
      CMD.cmd("pdftotext #{pdf_file} -", :pipe => false, :stderr => true)
    end
  end
end
