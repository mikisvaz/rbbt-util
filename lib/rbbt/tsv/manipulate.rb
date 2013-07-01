require 'progress-bar'
require 'rbbt/persist'
require 'rbbt/tsv/util'
require 'set'

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
      @new_fields = case
                    when new_fields.nil?
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
                    when Array === new_fields
                      new_fields.collect do |field|
                        TSV.identify_field(key_field, fields, field)
                      end
                    when (String === new_fields or Symbol === new_fields)
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
      progress_monitor = Progress::Bar.new(size, 0, step, desc)
    else
      progress_monitor = nil
    end

    each do |key, value|
      progress_monitor.tick if progress_monitor

      keys, value = traverser.process(key, value)
      
      keys = [keys].compact unless Array === keys

      # Annotated with Entity and NamedArray
      if not @unnamed
        if not traverser.new_field_names.nil? 
          case type
          when :double, :list
            if value.frozen?
              Log.warn "Value frozen: #{ value }"
            end
            if value.nil?
              nil
            else
              NamedArray.setup value, traverser.new_field_names, key, entity_options, entity_templates
            end
          when :flat, :single
            prepare_entity(value, traverser.new_field_names.first, entity_options)
          end
        end
      end

      next if keys.nil?

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

    [traverser.new_key_field_name, traverser.new_field_names]
  end

  def reorder(new_key_field = nil, new_fields = nil, options = {}) 
    zipped, uniq = Misc.process_options options, :zipped, :uniq

    persist_options = Misc.pull_keys options, :persist
    persist_options[:prefix] = "Reorder"

    Persist.persist_tsv self, self.filename, {:key_field => new_key_field, :fields => new_fields}, persist_options do |data|

      with_unnamed do
        new_key_field_name, new_field_names = through new_key_field, new_fields, uniq, zipped do |key, value|
          if data.include?(key) and not zipped
            case type 
            when :double
              data[key] = data[key].zip(value).collect do |old_list, new_list| old_list + new_list end
            when :flat
              data[key].concat value
            end
          else
            data[key] = value.dup
          end
        end

        data.extend TSV unless TSV === data
        data.key_field = new_key_field_name
        data.fields = new_field_names
        data.filename = filename
        data.namespace = namespace
        data.entity_options = entity_options
        data.entity_templates = entity_templates
        data.type = type
      end
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

  def select(method = nil, &block)
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
        new[key] = values if yield key, values
      end
    when Array === method
      method = Set.new method
      with_unnamed do
        case type
        when :single
          through do |key, value|
            new[key] = value if method.include? key or method.include? value
          end
        when :list, :flat
          through do |key, values|
            new[key] = values if method.include? key or (method & values).any?
          end
        else
          through do |key, values|
            new[key] = values if method.include? key or (method & values.flatten).any?
          end
        end
      end
    when Regexp === method
      with_unnamed do
        through do |key, values|
          new[key] = values if [key,values].flatten.select{|v| v =~ method}.any?
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
                new[key] = values if yield(key)
              end
            when (type == :single or type == :flat)
              through do |key, value|
                new[key] = value if yield(value)
              end
            else
              pos = identify_field method
              raise "Field #{ method } not identified. Available: #{ fields * ", " }" if pos.nil?

              through do |key, values|
                new[key] = values if yield(values[pos])
              end
            end
          end
        when block.arity == 2
          with_unnamed do
            case
            when (method == key_field or method == :key)
              through do |key, values|
                new[key] = values if yield(key, key)
              end
            when (type == :single or type == :flat)
              through do |key, value|
                new[key] = value if yield(key, value)
              end
            else
              pos = identify_field method
              through do |key, values|
                new[key] = values if yield(key, values[pos])
              end
            end

          end

        end


      else
        with_unnamed do
          through do |key, values|
            new[key] = values if [key,values].flatten.select{|v| v == method}.any?
          end
        end
      end
    when Hash === method
      key  = method.keys.first
      method = method.values.first
      case
      when (Array === method and (key == :key or key_field == key))
        with_unnamed do
          method.each{|key| 
            new[key] = self[key] if self.include? key
          }
        end
      when Array === method
        with_unnamed do
          method = Set.new method unless Set === method
          case type
          when :single
            through :key, key do |key, value|
              new[key] = self[key] if method.include? value
            end
          when :list
            through :key, key do |key, values|
              new[key] = self[key] if method.include? values.first
            end
          when :flat #untested
            through :key, key do |key, values|
              new[key] = self[key] if (method & values.flatten).any?
            end
          else
            through :key, key do |key, values|
              new[key] = self[key] if (method & values.flatten).any?
            end
          end
        end
      when Regexp === method
        with_unnamed do
          through :key, key do |key, values|
            values = [values] if type == :single
            new[key] = self[key] if values.flatten.select{|v| v =~ method}.any?
          end
        end
      when String === method
        with_unnamed do
          through :key, key do |key, values|
            values = [values] if type == :single
            new[key] = self[key] if values.flatten.select{|v| v == method}.any?
          end
        end
      when Proc === method
        with_unnamed do
          through :key, key do |key, values|
            values = [values] if type == :single
            new[key] = self[key] if values.flatten.select{|v| method.call(v)}.any?
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
      next if values.nil?

      case
      when type == :single
        field_values = values
      when type == :flat
        field_values = values
      else
        next if values[field_pos].nil? 
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
      when NamedArray === values
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

  def transpose(key_field)
    raise "Transposing only works for TSVs of type :list" unless type == :list
    new_fields = keys
    new = TSV.setup({}, :key_field => key_field, :fields => new_fields, :type => type, :filename => filename, :identifiers => identifiers)

    through do |key, values|
      fields.zip(values) do |new_key, value|
        new[new_key] ||= []
        new[new_key][new_fields.index key] = value
      end
    end

    new.entity_options = entity_options
    new.entity_templates = entity_templates
    new.namespace = namespace

    new
  end
end
