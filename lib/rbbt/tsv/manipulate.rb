require 'progress-bar'
require 'rbbt/persist'
require 'rbbt/tsv/util'

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
                   else
                     if Array === new_fields
                       new_fields.collect do |field|
                         TSV.identify_field(key_field, fields, field)
                       end
                     else
                       [TSV.identify_field(key_field, fields, new_fields)]
                     end
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
        if type == :double
          if uniq
            self.instance_eval do alias process process_reorder_double_uniq end
          else
            self.instance_eval do alias process process_reorder_double end
          end
        else
          self.instance_eval do alias process process_reorder_list end
        end
      end
    end

  end

  #{{{ Methods

  def through(new_key_field = nil, new_fields = nil, uniq = false)

    traverser = Traverser.new @key_field, @fields, new_key_field, new_fields, type, uniq

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
      next if keys.nil?
      keys.each do |key|
        NamedArray.setup value, traverser.new_field_names if Array === value and not @unnamed
        yield key, value
      end
    end

    [traverser.new_key_field_name, traverser.new_field_names]
  end

  def reorder(new_key_field = nil, new_fields = nil, persist = false)
    Persist.persist_tsv self, self.filename, {:key_field => new_key_field, :fields => new_fields}, {:persist => persist, :persist_prefix => "Reorder:"} do |data|

      with_unnamed do
        new_key_field_name, new_field_names = through new_key_field, new_fields do |key, value|
          if data.include?(key) and type == :double
            data[key] = data[key].zip(value).collect do |old_list, new_list| old_list + new_list end
          else
            data[key] = value
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
      with_unnamed do
        through do |key, values|
          new[key] = values if ([key,values].flatten & method).any?
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
          through :key, key do |key, values|
            values = [values] if type == :single
            new[key] = self[key] if (values.flatten & method).any?
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
        values[field].replace new_values
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
