require 'iconv'
require 'lockfile'
require 'digest/md5'

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
  def indiferent_get(key)
    old_get(key) || 
    old_get(key.to_s) || 
    old_get(key.to_sym) 
  end

  def self.extended(base)
    class << base
      alias_method :old_get, :[]
      alias_method :[], :indiferent_get
    end
  end
end

module Misc
  class FieldNotFoundError < StandardError;end

  def self.filename?(filename)
    String === filename and filename.length < 1024 and filename.index("\n").nil? and File.exists? filename
  end

  def self.lock(file, *args)
    FileUtils.mkdir_p File.dirname(File.expand_path(file)) unless File.exists?  File.dirname(File.expand_path(file))
    lockfile = Lockfile.new(file + '.lock')
    lockfile.lock do
      yield file, *args
    end
  end

  def self.string2const(string)
    return nil if string.nil?
    mod = Kernel

    string.to_s.split('::').each do |str|
      mod = mod.const_get str
    end

    mod
  end

  def self.path_relative_to(path, subdir)
    File.expand_path(path).sub(/^#{Regexp.quote File.expand_path(subdir)}\/?/,'')
  end

  def self.in_directory?(file, directory)
    if File.expand_path(file) =~ /^#{Regexp.quote File.expand_path(directory)}/
      true
    else
      false
    end
  end

  def self.find_files_back_to(path, target, subdir)
    return [] if path.nil?
    files = []
    while in_directory?(path, subdir)
      path = path.dirname
      if path[target].exists?
        files << path[target]
      end
    end

    files
  end

  def self.this_dir
    File.expand_path(File.dirname(caller[0]))
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

  def self.count(list)
    counts = Hash.new 0
    list.each do |item|
      counts[item] += 1
    end

    counts
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

  def self.fixutf8(string)
    if string.respond_to?(:valid_encoding?) and ! string.valid_encoding?
      @@ic ||= Iconv.new('UTF-8//IGNORE', 'UTF-8')
      @@ic.iconv(string)
    else
      string
    end
  end

  def self.add_defaults(options, defaults = {})
    case
    when Hash === options
      new_options = options.dup
    when String === options
      new_options = string2hash options
    else
      raise "Format of '#{options.inspect}' not understood"
    end
    defaults.each do |key, value|
      next unless new_options[key].nil?

      new_options[key] = value 
    end
    new_options
  end

  def self.process_options(hash, *keys)
    if keys.length == 1
      hash.delete keys.first.to_sym
    else
      keys.collect do |key| hash.delete(key.to_sym) || hash.delete(key.to_s) end
    end
  end

  def self.hash2string(hash)
    hash.sort_by{|k,v| k.to_s}.collect{|k,v| 
      next unless %w(Symbol String Float Fixnum Integer TrueClass FalseClass Module Class Object).include? v.class.to_s
      [ Symbol === k ? ":" << k.to_s : k,
        Symbol === v ? ":" << v.to_s : v] * "="
    }.compact * "#"
  end

  def self.hash2md5(hash)
    o = {}
    hash.keys.sort_by{|k| k.to_s}.each do |k|
      next if k == :monitor or k == "monitor" or k == :in_situ_persistence or k == "in_situ_persistence"
      v = hash[k]
      case
      when v.inspect =~ /:0x0/
        o[k] = v.inspect.sub(/:0x[a-f0-9]+@/,'')
      when Resource::Path === v
        o[k] = "" << String.new(v.to_s)
      else
        o[k] = v
      end
    end

    Digest::MD5.hexdigest(o.sort_by{|k| k.to_s}.inspect)
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
          when (String === value and value =~ /^\/(.*)\/$/)
            Regexp.new /#{$1}/
          else
            begin
              Kernel.const_get value
            rescue
              begin  
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

  def self.field_position(fields, field, quiet = false)
    return field if Integer === field or Range === field
    raise FieldNotFoundError, "Field information missing" if fields.nil? && ! quiet
    fields.each_with_index{|f,i| return i if f == field}
    field_re = Regexp.new /#{field}/i
    fields.each_with_index{|f,i| return i if f =~ field_re}
    raise FieldNotFoundError, "Field #{ field.inspect } was not found" unless quiet
  end

  def self.first(list)
    return nil if list.nil?
    return list.first
  end

  def self.chunk(text, split)
    text.split(split)[1..-1]
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


  def self.process_to_hash(list)
    result = yield list
    hash = {}
    list.zip(result).each do |k,v| hash[k] = v end
    hash
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
end

module PDF2Text
  def self.pdf2text(filename)
    require 'rbbt/util/cmd'
    require 'rbbt/util/tmpfile'
    require 'rbbt/util/open'


    CMD.cmd("pdftotext - -", :in => Open.open(filename, :nocache => true), :pipe => true, :stderr => true)
  end
end

class NamedArray < Array
  attr_accessor :fields

  def self.name(array, fields)
    a = self.new(array)
    a.fields = fields
    a
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

  alias original_get_brackets []
  def [](key)
    original_get_brackets(Misc.field_position(fields, key))
  end

  alias original_set_brackets []=
    def []=(key,value)
      original_set_brackets(Misc.field_position(fields, key), value)
    end

  alias original_values_at values_at
  def values_at(*keys)
    keys = keys.collect{|k| Misc.field_position(fields, k) }
    original_values_at(*keys)
  end

  def zip_fields
    return [] if self.empty?
    zipped = self[0].zip(*self[1..-1])
    zipped = zipped.collect{|v| NamedArray.name(v, fields)}
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
      "* #{ field }: #{ Array === value ? value * "|" : value }"
    end * "\n"
  end

end

def benchmark(bench = true)
  require 'benchmark'
  if bench
    res = nil
    puts(Benchmark.measure do
      res = yield
    end)
    res
  else
    yield
  end
end

def profile(prof = true)
  require 'ruby-prof'
  if prof
    RubyProf.start
    res = yield
    result = RubyProf.stop

    # Print a flat profile to text
    printer = RubyProf::FlatPrinter.new(result)
    printer.print(STDOUT, 0)
    res
  else
    yield
  end
end
