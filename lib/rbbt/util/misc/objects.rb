module Misc

  def self.prepare_entity(entity, field, options = {})
    return entity unless defined? Entity
    return entity unless String === entity or Array === entity
    options ||= {}

    dup_array = options.delete :dup_array

    if Annotated === field or Entity.respond_to?(:formats) and Entity.formats.include? field
      params = options.dup

      params[:format] ||= params.delete "format"
      params.merge!(:format => field) unless params.include?(:format) and not ((f = params[:format]).nil? or (String === f and f.empty?))

      mod = Entity === field ? field : Entity.formats[field]
      entity = mod.setup(
        ((entity.frozen? and not entity.nil?) ? entity.dup : ((Array === entity and dup_array) ? entity.collect{|e| e.nil? ? e : e.dup} : entity) ),
        params
      ) 
    end

    entity
  end

  def self.consolidate(list)
    list.inject(nil){|acc,e|
      if acc.nil?
        acc = e
      else
        acc.concat e
        acc
      end
    }
  end

  def self.append_zipped(current, new)
    current.each do |v|
      n = new.shift
      if Array === n
        v.concat new
      else
        v << n
      end
    end
    current
  end

  def self.zip_fields(array)
    return [] if array.empty? or (first = array.first).nil?
    first.zip(*array[1..-1])
  end

  def self.field_position(fields, field, quiet = false)
    return field if Integer === field or Range === field
    raise FieldNotFoundError, "Field information missing" if fields.nil? && ! quiet
    fields.each_with_index{|f,i| return i if f == field}
    field_re = Regexp.new /^#{field}$/i
    fields.each_with_index{|f,i| return i if f =~ field_re}
    raise FieldNotFoundError, "Field #{ field.inspect } was not found" unless quiet
  end
end

class Hash
  def chunked_values_at(keys, max = 5000)
    Misc.ordered_divide(keys, max).inject([]) do |acc,c|
      new = self.values_at(*c)
      new.annotate acc if new.respond_to? :annotate and acc.empty?
      acc.concat(new)
    end
  end
end

module LaterString
  def to_s
    yield
  end
end

