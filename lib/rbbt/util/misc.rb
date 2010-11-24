
module Misc
  class FieldNotFoundError < StandardError;end

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
    new_options = options.dup
    defaults.each do |key, value|
      new_options[key] = value if new_options[key].nil?
    end
    new_options
  end

  def self.string2hash(string)
    hash = {}
    string.split(',').each do |part|
      key, value = part.split('=>')
      hash[key] = value
    end
    
    hash
  end

  def self.sensiblewrite(path, content)
    if String === content
      File.open(path, 'w') do |f|  f.write content  end
    else
      File.open(path, 'w') do |f|  while l = content.gets; f.write l; end  end
    end
  end

  def self.field_position(fields, field, quiet = false)
    return field if Integer === field or Range === field
    raise FieldNotFoundError, "Field information missing" if fields.nil? && ! quiet
    fields.each_with_index{|f,i| return i if f == field}
    field_re = Regexp.new /#{field}/i
    fields.each_with_index{|f,i| return i if f =~ field_re}
    raise FieldNotFoundError, "Field #{ field } was not found" unless quiet
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
end


def profile 
  require 'ruby-prof'
  RubyProf.start
  yield
  result = RubyProf.stop

  # Print a flat profile to text
  printer = RubyProf::FlatPrinter.new(result)
  printer.print(STDOUT, 0)
end
