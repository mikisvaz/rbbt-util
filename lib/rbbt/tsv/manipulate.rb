require 'progress-bar'

module TSV
  
  attr_accessor :monitor

  class Traverser
    attr_accessor :new_key_field, :new_fields, :new_key_field_name, :new_field_names, :type, :uniq

    def process_null(key, values)
      [[key], values]
    end

    def process_subset_list(key, values)
      [key, @new_fields.collect{|field| field == :key ?  key : values[field] }]
    end

    def process_subset_all_but_list(key, values)
      new = values.dup
      new.delete_at(0 - @new_fields)
      [key, new]
    end

    def process_reorder_single(key, values)
      new_key = @new_key_field == :key ? key : values
      new_value = @new_fields.collect{|field| field == :key ?  key : values }.first
      return [new_key, new_value]
      [ [values[@new_key_field]], 
        @new_fields.collect{|field| field == :key ?  key : values[field] }]
    end

    def process_reorder_list(key, values)
      [ [values[@new_key_field]], 
        @new_fields.collect{|field| field == :key ?  key : values[field] }]
    end

    def process_reorder_double_uniq(key, values)
      [ values[@new_key_field].uniq, 
        @new_fields.collect{|field| field == :key ?  
          [key] : values[field] }
      ]
    end

    def process_subset_double(key, values)
      [[key], @new_fields.collect{|field| field == :key ?  [key] : values[field] }]
    end

    def process_subset_all_but_double(key, values)
      new = values.dup
      new.delete_at(0 - @new_fields)
      [[key], new]
    end

    def process_reorder_double(key, values)
      [ values[@new_key_field], 
        @new_fields.collect{|field| field == :key ?  
          [key] : values[field] }
      ]
    end

    def process_reorder_flat(key, values)
      [ values,
        @new_fields.collect{|field| field == :key ?  
          [key] : values[field] }.flatten
      ]
    end

    def initialize(key_field, fields, new_key_field, new_fields, type, uniq)
      @new_key_field = TSV.identify_field(key_field, fields, new_key_field)

      raise "Key field #{ new_key_field } not found" if @new_key_field.nil?
      @new_fields = case new_fields
                    when nil
                      case 
                      when @new_key_field == :key
                        :all
                      when fields.nil?
                        - @new_key_field
                      else 
                        new = (0..fields.length - 1).to_a
                        new.delete_at(@new_key_field)
                        new.unshift :key
                        new
                      end
                    when Array
                      new_fields.collect do |field|
                        TSV.identify_field(key_field, fields, field)
                      end
                    when String, Symbol
                      [TSV.identify_field(key_field, fields, new_fields)]
                    else
                      raise "Unknown format for new_fields (should be nil, Array or String): #{new_fields.inspect}"
                    end

      @new_key_field_name = case 
                            when @new_key_field == :key
                              key_field
                            else
                              fields[@new_key_field] if Array === fields
                            end

      if Array === fields
        @new_field_names = case
                           when fields.nil?
                             nil
                           when Array === @new_fields
                             @new_field_names = @new_fields.collect do |field|
                               case 
                               when field == :key
                                 key_field
                               else
                                 fields[field]
                               end
                             end
                           when @new_fields == :all
                             fields
                           when (Fixnum === @new_fields and @new_fields <= 0)
                             new = fields.dup
                             new.delete_at(- @new_fields)
                             new.unshift key_field
                             new
                           end
      end

      case
      when (@new_key_field == :key and (@new_fields == :all or fields.nil? or @new_fields == (0..fields.length - 1).to_a))
        self.instance_eval do alias process process_null end
      when @new_key_field == :key 
        if type == :double
          if Fixnum === @new_fields and @new_fields <= 0
            self.instance_eval do alias process process_subset_all_but_double end
          else
            self.instance_eval do alias process process_subset_double end
          end
        else
          if Fixnum === @new_fields and @new_fields <= 0
            self.instance_eval do alias process process_subset_all_but_list end
          else
            self.instance_eval do alias process process_subset_list end
          end
        end
      else
        case type 
        when :double
          if uniq
            self.instance_eval do alias process process_reorder_double_uniq end
          else
            self.instance_eval do alias process process_reorder_double end
          end
        when :flat
          self.instance_eval do alias process process_reorder_flat end
        when :single
          self.instance_eval do alias process process_reorder_single end
        else
          self.instance_eval do alias process process_reorder_list end
        end
      end
    end

  end

  #{{{ Methods

  def through(new_key_field = nil, new_fields = nil, uniq = false, zipped = false)

    traverser = Traverser.new key_field, fields, new_key_field, new_fields, type, uniq

    if @monitor
      desc = "Iterating TSV"
      step = 100
      if Hash === @monitor
        desc = @monitor[:desc] if @monitor.include? :desc 
        step = @monitor[:step] if @monitor.include? :step 
      end
      progress_monitor = Log::ProgressBar.new_bar(size, :desc => desc)
    else
      progress_monitor = nil
    end

    each do |key, value|
      progress_monitor.tick if progress_monitor
      next if value.nil?

      keys, value = traverser.process(key, value)

      next if keys.nil?
      
      keys = [keys].compact unless Array === keys

      # Annotated with Entity and NamedArray
      if not @unnamed and not traverser.new_field_names.nil? 

        case type
        when :double, :list
          Log.warn "Value frozen: #{ value }" if value.frozen?

          value.nil? ?
            nil :
            NamedArray.setup(value, traverser.new_field_names, key, entity_options, entity_templates)

        when :flat, :single
          prepare_entity(value, traverser.new_field_names.first, entity_options)
        end
      end



      if zipped

        keys.each_with_index do |k,i|
          v = value.collect{|v|
            r = v[i]
            r = v[0] if r.nil?
            r
          }

          if not @unnamed 
            k = Misc.prepare_entity(k, traverser.new_key_field_name, entity_options)
          end
          v.key = k if NamedArray === v
          yield k, v
 
        end

      else

        keys.each do |key|
          if not @unnamed
            k = Misc.prepare_entity(k, traverser.new_key_field_name, entity_options)
          end
          value.key = key if NamedArray === value
          yield key, value
        end

      end

    end

    Log::ProgressBar.remove_bar progress_monitor if progress_monitor

    [traverser.new_key_field_name, traverser.new_field_names]
  end

  def reorder(new_key_field = nil, new_fields = nil, options = {}) 
    zipped, uniq, merge = Misc.process_options options, :zipped, :uniq, :merge

    persist_options = Misc.pull_keys options, :persist
    persist_options[:prefix] = "Reorder"

    Persist.persist_tsv self, self.filename, self.options.merge({:key_field => new_key_field, :fields => new_fields}), persist_options do |data|
      if data.respond_to? :persistence_path
        real_data = data 
        data = {}
      end

      new_key_field_name, new_field_names = nil, nil
      with_unnamed do
        if zipped or (type != :double and type != :flat)
          new_key_field_name, new_field_names = through new_key_field, new_fields, uniq, zipped do |key, value|
            if merge 
              if data[key]
                new_values = data[key].dup
                value.each_with_index do |v,i|
                  new_values[i] = [new_values[i], v].flatten
                end
                data[key] = new_values if Array === value
              else
                data[key] = value.collect{|v| [v]} if Array === value
              end
            else
              data[key] = value.clone if Array === value
            end
          end
        else
          case type 
          when :double
            new_key_field_name, new_field_names = through new_key_field, new_fields, uniq, zipped do |keys, value|
              keys = [keys] unless Array === keys
              keys.each do |key|
                if data[key] 
                  current = data[key].dup
                  value.each_with_index do |v, i|
                    if current[i]
                      current[i] += v if v
                    else
                      current[i] = v || []
                    end
                  end
                  data[key] = current 
                else
                  data[key] = value.collect{|v| v.nil? ? nil : v.dup}
                end
              end
            end
          when :flat
            new_key_field_name, new_field_names = through new_key_field, new_fields, uniq, zipped do |key, value|
              data[key] ||= []
              data[key] += value
            end
          end
        end
      end

      if real_data and real_data.respond_to? :persistence_path
        real_data.serializer = type if real_data.respond_to? :serializer
        real_data.merge!(data)
        data = real_data
      end

      data.extend TSV unless TSV === data
      self.annotate(data)
      data.entity_options = self.entity_options
      data.entity_templates = self.entity_templates

      data.key_field = new_key_field_name
      data.fields = new_field_names
      data.fields.each do |field|
        data.entity_templates[field] = entity_templates[field] if entity_templates.include? field
      end
      data.type = zipped ? (merge ? :double : :list) : type
    end
  end

  def slice(fields)
    reorder :key, fields
  end

  def sort(*fields)
    fields = nil if fields.empty?

    elems = []
    through :key, fields do |key, value|
      elems << case
      when block_given?
        [key, yield(*value)]
      else
        case
        when type == :single
          [key, value]
        when type == :double
          [key, value.first.first]
        else
          [key, value.first]
        end
      end
    end

    elems.sort_by{|k,v| v}.collect{|k,v| k}
  end

  def select(method = nil, invert = false, &block)
    new = TSV.setup({}, :key_field => key_field, :fields => fields, :type => type, :filename => filename, :identifiers => identifiers)

    new.key_field = key_field
    new.fields    = fields.dup unless fields.nil?
    new.type      = type
    new.filename  = filename
    new.namespace = namespace
    new.entity_options = entity_options
    new.entity_templates = entity_templates
    
   case
    when (method.nil? and block_given?)
      through do |key, values|
        new[key] = values if invert ^ (yield key, values)
      end
    when Array === method
      method = Set.new method
      with_unnamed do
        case type
        when :single
          through do |key, value|
            new[key] = value if invert ^ (method.include? key or method.include? value)
          end
        when :list, :flat
          through do |key, values|
            new[key] = values if invert ^ (method.include? key or (method & values).any?)
          end
        else
          through do |key, values|
            new[key] = values if invert ^ (method.include? key or (method & values.flatten).any?)
          end
        end
      end
    when Regexp === method
      with_unnamed do
        through do |key, values|
          new[key] = values if invert ^ ([key,values].flatten.select{|v| v =~ method}.any?)
        end
      end
    when String === method
      if block_given?
        case 
        when block.arity == 1
          with_unnamed do
            case
            when (method == key_field or method == :key)
              through do |key, values|
                new[key] = values if invert ^ (yield(key))
              end
            when (type == :single or type == :flat)
              through do |key, value|
                new[key] = value if invert ^ (yield(value))
              end
            else
              pos = identify_field method
              raise "Field #{ method } not identified. Available: #{ fields * ", " }" if pos.nil?

              through do |key, values|
                new[key] = values if invert ^ (yield(values[pos]))
              end
            end
          end
        when block.arity == 2
          with_unnamed do
            case
            when (method == key_field or method == :key)
              through do |key, values|
                new[key] = values if invert ^ (yield(key, key))
              end
            when (type == :single or type == :flat)
              through do |key, value|
                new[key] = value if invert ^ (yield(key, value))
              end
            else
              pos = identify_field method
              through do |key, values|
                new[key] = values if invert ^ (yield(key, values[pos]))
              end
            end

          end
        end

      else
        with_unnamed do
          through do |key, values|
            new[key] = values if invert ^ ([key,values].flatten.select{|v| v == method}.any?)
          end
        end
      end
    when Hash === method
      key  = method.keys.first
      method = method.values.first
      case
      when (Array === method and (key == :key or key_field == key))
        with_unnamed do
          Annotated.purge(method).each{|key| 
            new[key] = self[key] if invert ^ (self.include? key)
          }
        end
      when Array === method
        with_unnamed do
          method = Set.new method unless Set === method
          case type
          when :single
            through :key, key do |key, value|
              new[key] = self[key] if invert ^ (method.include? value)
            end
          when :list
            through :key, key do |key, values|
              new[key] = self[key] if invert ^ (method.include? values.first)
            end
          when :flat #untested
            through :key, key do |key, values|
              new[key] = self[key] if invert ^ ((method & values.flatten).any?)
            end
          else
            through :key, key do |key, values|
              new[key] = self[key] if invert ^ ((method & values.flatten).any?)
            end
          end
        end

      when Regexp === method
        with_unnamed do
          through :key, key do |key, values|
            values = [values] if type == :single
            new[key] = self[key] if invert ^ (values.flatten.select{|v| v =~ method}.any?)
          end
        end

      when (String === method and method =~ /name:(.*)/)
        name = $1
        old_unnamed = self.unnamed
        self.unnamed = false
        if name.strip =~ /^\/(.*)\/$/
          regexp = Regexp.new $1
          through :key, key do |key, values|
            case type
            when :single
              values = values.annotate([values])
            when :double
              values = values[0]
            end
            new[key] = self[key] if invert ^ (values.select{|v| v.name =~ regexp}.any?)
          end
        else
          through :key, key do |key, values|
            case type
            when :single
              values = values.annotate([values])
            when :double
              values = values[0]
            end
            new[key] = self[key] if invert ^ (values.select{|v| v.name == name}.any?)
          end
        end
        self.unnamed = old_unnamed

      when String === method
        with_unnamed do
          through :key, key do |key, values|
            values = [values] if type == :single
            new[key] = self[key] if invert ^ (values.flatten.select{|v| v == method}.any?)
          end
        end

      when Fixnum === method
        with_unnamed do
          through :key, key do |key, values|
            new[key] = self[key] if invert ^ (values.flatten.length >= method)
          end
        end
      when Proc === method
        with_unnamed do
          through :key, key do |key, values|
            values = [values] if type == :single
            new[key] = self[key] if invert ^ (values.flatten.select{|v| method.call(v)}.any?)
          end
        end
      end
    end

    new
  end

  def column(field, cast = nil)
    new = slice(field)

    new.with_unnamed do
      new.each do |k,v|
        nv = v.first 
        nv = nv.send(cast) unless cast.nil?
        new[k] = nv
      end
    end

    case type
    when :double, :flat
      new.type = :flat
    else
      new.type = :single
    end

    new
  end

  def column_values(field, options = {})
    all = []
    through :key, field do |k,values|
      values = Array === values ? values.flatten : [values]
      all.concat values
    end
    prepare_entity(all, field, options = {})
  end


  def process_key(&block)
    new = annotate({})
    through do |key, values|
      key = case 
            when block.arity == 1
              yield(key)
            when block.arity == 2
              yield(key, values)
            else
              raise "Unexpected arity in block, must be 1, 2 or 3: #{block.arity}"
            end
      new[key] = values
    end
    new
  end

  def process(field, &block)
    field_pos = identify_field field

    through do |key, values|
      case
      when type == :single
        field_values = values
      when type == :flat
        field_values = values
      else
        next if values.nil?
        field_values = values[field_pos]
      end

      new_values = case 
                   when block.arity == 1
                     yield(field_values)
                   when block.arity == 2
                     yield(field_values, key)
                   when block.arity == 3
                     yield(field_values, key, values)
                   else
                     raise "Unexpected arity in block, must be 1, 2 or 3: #{block.arity}"
                   end

      case
      when type == :single
        self[key] = new_values
      when type == :flat
        self[key] = new_values
      else
        if (String === values[field_pos] and String === new_values) or
          (Array === values[field_pos] and Array === new_values) 
           values[field_pos].replace new_values
        else
          values[field_pos] = new_values
        end
        self[key] = values
      end
    end

    self
  end

  def add_field(name = nil)
    old_monitor = @monitor
    @monitor = {:desc => "Adding field #{ name }"} if TrueClass === monitor

    through do |key, values|
      new_values = yield(key, values)
      new_values = [new_values] if type == :double and not Array === new_values

      case
      when (values.nil? and (fields.nil? or fields.empty?))
        values = [new_values]
      when values.nil?  
        values = [nil] * fields.length + [new_values]
      when Array === values
        values += [new_values]
      else
        values << new_values
      end

      self[key] = values
    end
    @monitor = old_monitor

    if not fields.nil? and not name.nil?
      new_fields = self.fields + [name]
      self.fields = new_fields
    end

    self
  end

  def add_fields(names = [])
    old_monitor = @monitor
    @monitor = {:desc => "Adding field #{ names * ", " }"} if TrueClass === monitor

    through do |key, values|
      values ||= fields ? [nil] * fields : []
      new_values = yield(key, values)

      case type
      when :double
        new_values = new_values.collect{|v| [v] } if Array === new_values and new_values.first and not Array === new_values.first
        values += new_values || [nil] * names.length
      when :list
        values += new_values || [nil] * names.length
      end

      self[key] = values
    end
    @monitor = old_monitor

    if not fields.nil? and not (names.nil? or names.empty?)
      new_fields = self.fields + names
      self.fields = new_fields
    end

    self
  end


  def transpose(key_field="Unkown ID")
    raise "Transposing only works for TSVs of type :list" unless type == :list
    new_fields = keys
    new = self.annotate({})
    TSV.setup(new, :key_field => key_field, :fields => new_fields, :type => type, :filename => filename, :identifiers => identifiers)

    through do |key, values|
      fields.zip(values) do |new_key, value|
        new[new_key] ||= []
        new[new_key][new_fields.index key] = value
      end
    end

    new
  end
end
