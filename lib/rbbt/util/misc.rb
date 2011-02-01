require 'iconv'

class RBBTError < StandardError
  attr_accessor :info

  alias old_to_s to_s
  def to_s
    str = old_to_s
    if info
      str << "\n" << "Additional Info:\n---\n" << info << "---"
    end
    str
  end
end

module Misc
  class FieldNotFoundError < StandardError;end

  def self.path_relative_to(path, subdir)
    File.expand_path(path).sub(/^#{Regexp.quote File.expand_path(subdir)}\/?/,'')
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
      new_options[key] = value if new_options[key].nil?
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
          $SAFE = 3;
          begin  
            if value =~ /\/(.*)\//
              Regexp.new /#{$1}/
            else
              eval(value) 
            end
          rescue 
            value 
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
    raise FieldNotFoundError, "Field '#{ field }' was not found" unless quiet
  end

  def self.first(list)
    return nil if list.nil?
    return list.first
  end

  def self.chunk(text, split)
    text.split(split)[1..-1]
  end

end

module PDF2Text
  def self.pdf2text(filename)
    require 'rbbt/util/cmd'
    require 'rbbt/util/tmpfile'
    require 'rbbt/util/open'
    TmpFile.with_file(Open.read(filename)) do |pdf|
      CMD.cmd("pdftotext #{pdf} -", :pipe => false, :stderr => true)
    end
  end
end

class NamedArray < Array
  attr_accessor :fields

  def self.name(array, fields)
    a = self.new(array)
    a.fields = fields
    a
  end

  def positions(fields)
    fields.collect{|field|
      Misc.field_position(@fields, field)
    }
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
    zipped = self[0].zip(*self[1..-1])
    zipped = zipped.collect{|v| NamedArray.name(v, fields)} if fields 
    zipped 
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
