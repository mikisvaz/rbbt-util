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

      # Annotated with Entity and NamedArray
      if not @unnamed
        if not traverser.new_field_names.nil? 
          case type
          when :double, :list
            NamedArray.setup value, traverser.new_field_names 
          when :flat, :single
            Entity.formats[traverser.new_field_names.first].setup(value, :format => traverser.new_field_names.first) if defined?(Entity) and Entity.respond_to?(:formats) and Entity.formats.include? traverser.new_field_names
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

          if not @unnamed and defined?(Entity) and not traverser.new_key_field_name.nil? and Entity.respond_to?(:formats) and Entity.formats.include? traverser.new_key_field_name
            k = Entity.formats[traverser.new_key_field_name].setup(k.dup, :format => traverser.new_key_field_name) 
          end
          v.key = k if NamedArray === v
          yield k, v
 
        end

      else
        keys.each do |key|
          if not @unnamed and defined?(Entity) and not traverser.new_key_field_name.nil? and Entity.respond_to?(:formats) and Entity.formats.include? traverser.new_key_field_name
            key = Entity.formats[traverser.new_key_field_name].setup(key.dup, :format => traverser.new_key_field_name) 
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

  def select(method = nil)
    new = TSV.setup({}, :key_field => key_field, :fields => fields, :type => type, :filename => filename, :identifiers => identifiers)

    new.key_field = key_field
    new.fields    = fields.dup
    new.type      = type
    new.filename  = filename
    
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
            new[key] = values if method.include? key or method.include? value
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
        pos = identify_field method
        with_unnamed do
          through do |key, values|
            new[key] = values if yield((method == key_field or method == :key)? key : values[pos])
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
              new[key] = self[key] if method.include? value.first
            end
          when :flat #untested
            through :key, key do |key, values|
              new[key] = self[key] if (method & values.flatten).any?
            end
          else
            through :key, key do |key, values|
              new[key] = self[key] if (method & values.first).any?
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
        values[field_pos].replace new_values
        self[key] = values
      end
    end
  end

  def add_field(name = nil)
    through do |key, values|
      new_values = yield(key, values)
      new_values = [new_values] if type == :double and not Array === new_values

      values << new_values
      self[key] = values
    end

    self.fields = self.fields + [name] if fields != nil and name != nil

    self
  end
end
