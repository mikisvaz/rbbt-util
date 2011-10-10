require 'rbbt/util/cmd'
module TSV
  class Parser
    attr_accessor :header_hash, :sep, :sep2, :type, :key_position, :field_positions, :cast, :key_field, :fields, :fix, :select, :serializer, :straight, :take_all, :zipped

    class SKIP_LINE < Exception; end
    class END_PARSING < Exception; end

    def all_fields
      all = [key_field] + fields
      NamedArray.setup all, all
    end

    def parse_header(stream)
      options = {}

      # Get line

      line = stream.gets
      raise "Empty content" if line.nil?
      line.chomp!

      # Process options line

      if line and line =~ /^#{@header_hash}: (.*)/
        options = Misc.string2hash $1
        line = stream.gets
      end

      # Determine separator

      @sep = options[:sep] if options[:sep]

      # Process fields line

      if line and line =~ /^#{@header_hash}/
        line.chomp!
        @fields = line.split(@sep)
        @key_field = @fields.shift
        @key_field = @key_field[(0 + header_hash.length)..-1] # Remove initial hash character
        line = stream.gets
      end

      @first_line = line

      options
    end

    def process(line)
      l = line.chomp
      raise Parser::SKIP_LINE if Proc === @select and not @select.call l
      l = @fix.call l if Proc === @fix
      raise Parser::END_PARSING unless l
      l
    end

    def cast?
      !! @cast
    end

    def chop_line(line)
      line.split(@sep, -1)
    end

    def get_values_single(parts)
      return parts.shift, parts.first if field_positions.nil?
      key = parts[key_position]
      value = parts[field_positions.first]
      [key, value]
    end

    def get_values_list(parts)
      return parts.shift, parts if field_positions.nil?
      key = parts[key_position]
      values = parts.values_at *field_positions
      [key, values]
    end

    def get_values_double(parts)
      return parts.shift.split(@sep2, -1), parts.collect{|value| value.split(@sep2, -1)} if field_positions.nil?
      keys = parts[key_position].split(@sep2, -1)
      values = parts.values_at(*field_positions).collect{|value| value.split(@sep2, -1)}
      [keys, values]
    end

    def get_values_flat(parts)
      return parts.shift.split(@sep2, -1), parts.collect{|value| value.split(@sep2, -1)} if field_positions.nil?
      keys = parts[key_position].split(@sep2, -1)
      if @take_all
        values = parts.collect{|value| value.split(@sep2, -1)}
      else
        values = parts.values_at(*field_positions).collect{|value| value.split(@sep2, -1)}
      end
      [keys, values]
    end

    def add_to_data_no_merge_list(data, key, values)
      data[key] = values unless data.include? key
    end

    def add_to_data_flat(data, keys, values)
      keys.each do |key|
        data[key] = values.flatten unless data.include? key
      end
    end

    def add_to_data_flat_merge(data, keys, values)
      keys.each do |key|
        if data.include? key
          data[key] = data[key].concat values.flatten 
        else
          data[key] = values.flatten
        end
      end
    end

    def add_to_data_no_merge_double(data, keys, values)
      keys.each do |key|
        data[key] = values unless data.include? key
      end
    end

    def add_to_data_merge(data, keys, values)
      keys.each do |key|
        if data.include? key
          data[key] = data[key].zip(values).collect do |old, new|
            old.concat new
            old
          end
        else
          data[key] = values
        end
      end
    end

    def add_to_data_merge_zipped(data, keys, values)
      num = keys.length
      values = values.collect{|v| v.length != num ? [v.first] * num : v}
      all = values.unshift keys
      Misc.zip_fields(all).each do |values|
        key = values.shift
        if data.include? key
          data[key] = data[key].zip(values).collect do |old, new|
            old.push new
            old
          end
        else
          data[key] = values.collect{|v| [v]}
        end
      end
    end

    def add_to_data_zipped(data, keys, values)
      num = keys.length
      values = values.collect{|v| v.length != num ? [v.first] * num : v}
      all = values.unshift keys
      Misc.zip_fields(all).each do |values|
        key = values.shift
        next if data.include? key
        data[key] = values.collect{|v| [v]}
      end
    end


    def cast_values_single(value)
      case
      when Symbol === cast
        value.send(cast)
      when Proc === cast
        cast.call value
      end
    end

    def cast_values_list(values)
      case
      when Symbol === cast
        values.collect{|v| v.send(cast)}
      when Proc === cast
        values.collect{|v| cast.call v}
      end
    end

    def cast_values_double(values)
      case
      when Symbol === cast
        values.collect{|list| list.collect{|v| v.send(cast)}}
      when Proc === cast
        values.collect{|list| list.collect{|v| cast.call v }}
      end
    end

    def rescue_first_line
      @first_line
    end

    def fix_fields(options)
      key_field = Misc.process_options options, :key_field
      fields    = Misc.process_options options, :fields

      if (key_field.nil? or key_field == 0 or key_field == :key) and
        (fields.nil? or fields == @fields or (not @fields.nil? and fields == (1..@fields.length).to_a))

        @straight = true
        return
      else
        @straight = false

        case
        when (key_field.nil? or key_field == @key_field or key_field == 0)
          @key_position = 0
        when Integer === key_field
          @key_position = key_field
        when String === key_field
          @key_position = @fields.dup.unshift(@key_field).index key_field
        else
          raise "Format of key_field not understood: #{key_field.inspect}"
        end

        if (fields.nil? or fields == @fields or (not @fields.nil? and fields == (1..@fields.length).to_a))
          if type != :flat
            @field_positions = (0..@fields.length).to_a
            @field_positions.delete @key_position
          end
        else
          fields = [fields] if not Array === fields
          @field_positions = fields.collect{|field|
            case
            when Integer === field
              field
            when String === field
              pos = @fields.dup.unshift(@key_field).index field
              raise "Field not identified: #{ field }" if pos.nil?
              pos
            else
              raise "Format of fields not understood: #{fields.inspect}"
            end
          }
        end

        new_key_field = @fields.dup.unshift(@key_field)[@key_position] if not @fields.nil?
        @fields = @fields.dup.unshift(@key_field).values_at *@field_positions if not @fields.nil? and not @field_positions.nil?
        @key_field = new_key_field
      end
    end

    def initialize(stream = nil, options = {})
      @header_hash = Misc.process_options(options, :header_hash) || "#"
      @sep = Misc.process_options(options, :sep) || "\t"

      options = parse_header(stream).merge options
      
      @type = Misc.process_options(options, :type) || :double
      merge = Misc.process_options(options, :merge) || false

      @sep2 = Misc.process_options(options, :sep2) || "|"
      @cast = Misc.process_options options, :cast
      @type ||= Misc.process_options options, :type
      @fix = Misc.process_options(options, :fix) 
      @select= Misc.process_options options, :select
      @zipped = Misc.process_options options, :zipped

      case @type
      when :double 
        self.instance_eval do alias get_values get_values_double end
        self.instance_eval do alias cast_values cast_values_double end
        case
        when (merge and not zipped)
            self.instance_eval do alias add_to_data add_to_data_merge end
        when (merge and zipped)
            self.instance_eval do alias add_to_data add_to_data_merge_zipped end
        when zipped
            self.instance_eval do alias add_to_data add_to_data_zipped end
        else
          self.instance_eval do alias add_to_data add_to_data_no_merge_double end
        end
      when :single
        self.instance_eval do alias get_values get_values_single end
        self.instance_eval do alias cast_values cast_values_single end
        self.instance_eval do alias add_to_data add_to_data_no_merge_list end
      when :list
        self.instance_eval do alias get_values get_values_list end
        self.instance_eval do alias cast_values cast_values_list end
        self.instance_eval do alias add_to_data add_to_data_no_merge_list end
      when :flat
        @take_all = true if options[:fields].nil?
        self.instance_eval do alias get_values get_values_flat end
        self.instance_eval do alias cast_values cast_values_double end
        if merge
          self.instance_eval do alias add_to_data add_to_data_flat_merge end
        else
          self.instance_eval do alias add_to_data add_to_data_flat end
        end
      end

      fix_fields(options)

      @straight = false if @sep != "\t" or not @cast.nil? or merge
    end

    def setup(data)
      data.extend TSV unless TSV === data
      data.type = @type
      data.key_field = @key_field
      data.fields = @fields
      data
    end
  end
end
