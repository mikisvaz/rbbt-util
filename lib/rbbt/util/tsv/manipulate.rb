class TSV

  attr_accessor :monitor
 
  def through(new_key_field = :key, new_fields = nil, &block)

    # Get positions

    new_key_position = identify_field new_key_field
    
    new_field_positions = case
                          when Integer === new_fields
                            [new_fields]
                          when String === new_fields
                            [identify_field(new_fields)]
                          when Array === new_fields
                            new_fields.collect{|new_field| identify_field new_field}
                          when new_fields == :key
                            [:key]
                          when new_fields == :fields
                            nil
                          when new_fields.nil?
                            nil
                          else
                            raise "Unknown new fields specified: #{new_fields.inspect}"
                          end

    # Get Field Names

    ## New key
    new_key_field_name = new_key_position == :key ? key_field : fields[new_key_position] if fields
    ## New fields
    new_field_names = case
                      when (new_field_positions.nil? and new_key_position == :key)
                        fields.dup
                      when new_field_positions.nil? 
                        f = fields.dup
                        f.delete_at(new_key_position)
                        f.unshift key_field
                        f
                      else
                        f = fields.dup
                        f.push key_field
                        f.values_at *new_field_positions.collect{|pos| pos == :key ? -1 : pos }
                      end if fields

    # Cycle through
    if monitor
      desc = "Iterating TSV"
      step = 100
      if Hash === monitor
        desc = monitor[:desc] if monitor.include? :desc 
        step = monitor[:step] if monitor.include? :step 
      end
      progress_monitor = Progress::Bar.new(size, 0, step, desc)
    else
      progress_monitor = nil
    end
                      
    if new_key_position == :key and ( new_fields.nil? or new_fields == fields)
      each do |key, fields| progress_monitor.tick if progress_monitor; yield key, fields end
    else
      each do |key, fields|
        progress_monitor.tick if progress_monitor;
        new_key_value = case
                        when (new_key_position.nil? or new_key_position == :key)
                          key
                        else
                          fields[new_key_position]
                        end
        new_field_values = case
                           when (new_field_positions.nil? and new_fields == :fields)
                             fields
                           when (new_fields.nil? and new_key_position == :key)
                             fields
                           when new_field_positions.nil?
                             f = fields.dup
                             f.delete_at(new_key_position)
                             if type == :double
                               f.unshift [key]
                             else
                               f.unshift key
                             end
                             f
                           else
                             f = fields.dup
                             if type == :double
                               f.push [key]
                             else
                               f.push key
                             end
                             f.values_at *new_field_positions.collect{|pos| pos == :key ? -1 : pos }
                           end
        new_field_values = NamedArray.name new_field_values, new_field_names

        next if new_key_value.nil? or (String === new_key_value and new_key_value.empty?)
        yield new_key_value, new_field_values
      end
    end

    # Return new field names

    return [new_key_field_name, new_field_names]
  end

  def reorder(new_key_field, new_fields = nil, options = {})
    options = Misc.add_defaults options, :new_key_field => new_key_field, :new_fields => new_fields, :persistence => false

    new, extra = Persistence.persist(self, :Reorder, :tsv, options ) do |tsv, options, filename|
      new_key_field = options[:new_key_field]
      new_fields    = options[:new_fields]

      new = {}
      new_key_field, new_fields = through new_key_field, new_fields do |key, values|
        if Array === key
          keys = key
        else
          keys = [key]
        end

        new_values = keys.each do |key|
          if new[key].nil?
            new[key] = values
          else
            if type == :double
              new[key] = new[key].zip(values).collect{|v| v.flatten}
            end
          end
        end
      end

      new = TSV.new new

      new.fields = new_fields
      new.key_field = new_key_field
      new.filename = filename
      new.type = type
      new.case_insensitive = case_insensitive
      new.identifiers = identifiers 

      new
    end 

    new
  end

  def slice(fields)
    reorder :key, fields
  end

  def slice_namespace(namespace)
    fields = self.fields
    namespace_fields = []
    fields.each_with_index{|field,i| namespace_fields << i if field.namespace == namespace}
    reorder :key, namespace_fields
  end

  def sort(*fields)

    pos = case
          when fields.length == 0
            :key
          when fields.length == 1
            identify_field fields.first
          else
            fields.collect{|field| identify_field field}
          end
    elems = []
    through :key, pos do |key, values|
      elems << case
      when block_given?
        [key, yield(values)]
      else
        if type == :double
          [key, values.first.first]
        else
          [key, values.first]
        end
      end
    end

    elems.sort_by{|k,v| v}.collect{|k,v| k}
  end

  def select(method = nil)
    new = TSV.new({})
    new.key_field = key_field
    new.fields    = fields.dup
    new.type      = type
    new.filename  = filename
    new.case_insensitive  = case_insensitive
    
   case
    when (method.nil? and block_given?)
      through do |key, values|
        new[key] = values if yield key, values
      end
    when Array === method
      through do |key, values|
        new[key] = values if ([key,values].flatten & method).any?
      end
    when Regexp === method
      through do |key, values|
        new[key] = values if [key,values].flatten.select{|v| v =~ method}.any?
      end
    when String === method
      if block_given?
        through do |key, values|
          new[key] = values if yield((method == key_field or method == :key)? key : values[method])
        end
      else
        through do |key, values|
          new[key] = values if [key,values].flatten.select{|v| v == method}.any?
        end
      end
    when Hash === method
      key  = method.keys.first
      method = method.values.first
      case
      when (Array === method and (key == :key or key_field == key))
        method.each{|item| new[item] = self[item] if self.include? item}
      when Array === method
        through :key, key do |key, values|
          new[key] = self[key] if (values.flatten & method).any?
        end
      when Regexp === method
        through :key, key do |key, values|
          new[key] = self[key] if values.flatten.select{|v| v =~ method}.any?
        end
      when String === method
        through :key, key do |key, values|
          new[key] = self[key] if values.flatten.select{|v| v == method}.any?
        end
      end
    end


    new
  end
 
  def process(field, &block)
    through do |key, values|
      if type == :flat
        field_values = values
      else
        field_values = values[field]
      end

      next if values[field].nil? 
      new_values = case 
                   when block.arity == 1
                     yield(field_values)
                   when block.arity == 2
                     yield(field_values, key)
                   when block.arity == 3
                     yield(field_values, key, values)
                   else
                     raise "Unknown arity in block"
                   end

      if type == :flat
        self[key] = new_values
      else
        values[field].replace new_values
      end
    end
  end

  def add_field(name = nil)
    through do |key, values|
      new_values = yield(key, values)
      new_values = [new_values] if type == :double and not Array === new_values

      self[key] = values + [new_values]
    end

    self.fields = self.fields + [name] if fields != nil and name != nil

    self
  end

  def add_fields(names = nil)
    through do |key, values|
      new_values = yield(key, values)
      new_values = [new_values] if type == :double and not Array == new_values

      self[key] = values.concat yield(key, values)
    end

    self.fields = self.fields.concat names if fields != nil and names != nil
  end
end
