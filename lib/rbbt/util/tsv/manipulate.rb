
class TSV

  def through(new_key_field = :key, new_fields = nil, &block)

    # Get positions

    new_key_position = identify_field new_key_field
    new_field_positions = case
                          when Integer === new_fields
                            [new_fields]
                          when String === new_fields
                            [identify_field new_fields]
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

    each do |key, fields|
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
      yield new_key_value, new_field_values
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
      new_key_field, new_fields = through new_key_field, new_fields do |key, values, filename|
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

      # TODO Fix the filename to be more informative
      [new, {:fields => new_fields, :key_field => new_key_field, :filename => "Reorder: #{ tsv.filename }", :type => tsv.type, :case_insensitive => tsv.case_insensitive}]
    end

    new = TSV.new new
    extra.each do |key, values| new.send("#{ key }=".to_sym, values) end if not extra.nil?

    new
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

  def select(method)
    new = TSV.new({})
    new.key_field = key_field
    new.fields    = fields.dup
    new.type      = type
    new.filename  = filename + "#Select: #{method.inspect}"
    new.case_insensitive  = case_insensitive
    
    case
    when Array === method
      through do |key, values|
        new[key] = values if ([key,values].flatten & method).any?
      end
    when Regexp === method
      through do |key, values|
        new[key] = values if [key,values].flatten.select{|v| v =~ method}.any?
      end
    when String === method
      through do |key, values|
        new[key] = values if [key,values].flatten.select{|v| v == method}.any?
      end
    when Hash === method
      key  = method.keys.first
      method = method.values.first
      case
      when (Array === method and (key == :key or key_field == key))
        method.each{|item| if values = self[item]; then  new[item] = values; end}
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
 
  def process(field)
    through do |key, values|
      values[field].replace yield(values[field], key, values) unless values[field].nil? 
    end
  end


  def add_field(name = nil)
    each do |key, values|
      new_values = yield(key, values)
      new_values = [new_values] if type == :double and not Array == new_values

      self[key] = values + [yield(key, values)]
    end

    self.fields = self.fields + [name] if fields != nil
  end


  def index(options = {})
    options = Misc.add_defaults options, :order => false, :persistence => false

    new, extra = Persistence.persist(self, :Index, :tsv, options) do |tsv, options, filename|
      new = {}
      if options[:order]
        new_key_field, new_fields = through options[:target], options[:others] do |key, values|
          if Array === key
            keys = key
          else
            keys = [key]
          end

          values.each_with_index do |list,i|
            list = [list] unless Array === list
            list.each do |elem|
              elem.downcase if options[:case_insensitive]
              new[elem] ||= []
              new[elem][i + 1] = (new[elem][i + 1] || []) + keys
            end
          end

          new[key]    ||= []
          new[options[:case_insensitive] ? key.downcase : key ][0] = (new[options[:case_insensitive] ? key.downcase : key ][0] || []) + keys

        end


        new.each do |key, values| 
          values.flatten!
          values.compact!
        end

      else
        new_key_field, new_fields = through options[:target], options[:others] do |key, values|
          if Array === key
            keys = key
          else
            keys = [key]
          end

          values.each do |list|
            list = [list] unless Array === list
            list.each do |elem|
              elem.downcase if options[:case_insensitive]
              new[elem] = (new[elem] || []) + keys
            end
          end
        end
      end

      [new, {:key_field => new_key_field, :fields => new_fields, :type => :list, :filename => (filename.nil? ? nil : "Index:" + filename), :case_insensitive => options[:case_insensitive]}]
    end

    new = TSV.new(new)
    new.filename = "Index: " + filename + options.inspect
    new.fields = extra[:fields]
    new.key_field = extra[:key_field]
    new.case_insensitive = extra[:case_insensitive]
    new.type = extra[:type]
    new
  end


end
