require 'rbbt/util/cmd'
module TSV
  class Parser
    attr_accessor :stream, :filename, :header_hash, :sep, :sep2, :type, :key_position, :field_positions, :cast, :key_field, :fields, :fix, :select, :serializer, :straight, :take_all, :zipped, :namespace, :first_line, :stream, :preamble, :identifiers

    class SKIP_LINE < Exception; end
    class END_PARSING < Exception; end

    def all_fields
      all = [key_field] + fields
      NamedArray.setup all, all
    end

    def parse_header(stream)
      options = {}
      @preamble = []

      # Get line

      #Thread.pass while IO.select([stream], nil, nil, 1).nil? if IO === stream
      line = stream.gets
      return {} if line.nil?
      #raise "Empty content: #{ stream.inspect }" if line.nil?
      line = Misc.fixutf8 line.chomp

      # Process options line

      if line and (String === @header_hash and line =~ /^#{@header_hash}: (.*)/)
        options = Misc.string2hash $1.chomp
        line = stream.gets
        line = Misc.fixutf8 line.chomp if line
      end

      # Determine separator

      @sep = options[:sep] if options[:sep]

      # Process fields line

      preamble << line if line
      while line and (TrueClass === @header_hash or (String === @header_hash and Misc.fixutf8(line) =~ /^#{@header_hash}/ ))
        @fields = line.split(@sep)
        @key_field = @fields.shift
        @key_field = @key_field[(0 + header_hash.length)..-1] if String === @header_hash

        #Thread.pass while IO.select([stream], nil, nil, 1).nil? if IO === stream
        line = (@header_hash != "" ?  stream.gets : nil)
        line = Misc.fixutf8 line.chomp if line
        preamble << line if line
        @header_hash = false if TrueClass === @header_hash
      end

      @preamble = preamble[0..-3] * "\n"

      line ||= stream.gets

      @first_line = line

      options
    end

    def process(line)
      l = line.chomp
      raise SKIP_LINE if l[0] == "#"[0] or (Proc === @select and not @select.call l)
      l = @fix.call l if Proc === @fix
      raise END_PARSING unless l
      l
    end

    def cast?
      !! @cast
    end

    def chop_line(line)
      @sep == " " ? line.split(/ /, -1) : line.split(@sep, -1)
    end

    def get_values_single_from_flat(parts)
      return parts.shift, parts.first if field_positions.nil? and key_position.nil?
      if key_position == 0
        [parts.shift, parts.first]
      else
        key = parts.shift
        [parts, key]
      end

    end

    def get_values_single(parts)
      return parts.shift, parts.first if field_positions.nil? and key_position.nil?
      key = parts[key_position]
      value = parts[(field_positions.nil? or field_positions.empty?) ? 0 : field_positions.first]
      [key, value]
    end

    def get_values_list(parts)
      return parts.shift, parts if field_positions.nil? and key_position.nil?
      key = parts[key_position]

      values = case
               when field_positions.nil?
                parts.tap{|o| o.delete_at key_position}
               when field_positions.empty?
                 []
               else
                parts.values_at *field_positions
               end

      [key, values]
    end

    def get_values_double(parts)
      return parts.shift.split(@sep2, -1), parts.collect{|value| value.split(@sep2, -1)} if field_positions.nil? and key_position.nil?
      keys = parts[key_position].split(@sep2, -1)
      values = case
               when field_positions.nil?
                parts.tap{|o| o.delete_at key_position}
               when field_positions.empty?
                 []
               else
                 parts.values_at *field_positions
               end.collect{|value| value.nil? ? [] : value.split(@sep2, -1) }
      [keys, values]
    end

    def get_values_flat_inverse(parts)
      value = parts.shift
      keys = parts
      [keys, [value]]
    end


    def get_values_flat_merge(parts)
      begin
        orig = parts

        if key_position and key_position != 0 and field_positions.nil?
          value = parts.shift.split(@sep2, -1)
          keys = parts.collect{|p| p.split(@sep2, -1) }.flatten
          return [keys, value]
        end

        return parts.shift.split(@sep2, -1), parts.collect{|value| value.split(@sep2, -1)}.flatten if 
        field_positions.nil? and (key_position.nil? or key_position == 0)
      rescue
        raise $!
      end

      str = parts[key_position]
      keys = str.split(@sep2, -1)

      if @take_all
        values = parts.collect{|e| e.split(@sep2, -1) }.flatten
      else
        if field_positions.nil?
          parts.delete_at key_position
          values = parts.first
        else
          values = parts[field_positions.first]
        end

        values = values.split(@sep2, -1)
      end

      [keys, values]
    end

    def get_values_flat(parts)
      keys, values = get_values_flat_merge(parts)
      [keys.first, values]
    end


    def add_to_data_no_merge_list(data, key, values)
      data[key] = values unless data.include? key
      nil
    end

    def add_to_data_flat_keys(data, key, values)
      data[key] = values unless data.include? key
      nil
    end

    def add_to_data_flat(data, key, values)
      data[key] = values unless data.include? key
      nil
    end

    def add_to_data_flat_merge(data, key, values)
      if data.include? key
        data[key] = data[key].concat values
      else
        data[key] = values
      end
      nil
    end

    def add_to_data_flat_merge_double(data, keys, values)
      keys.each do |key|
        if data.include? key
          data[key] = data[key].concat values
        else
          data[key] = values
        end
      end
      nil
    end

    def add_to_data_flat_merge_keys(data, keys, values)
      keys.each do |key|
        if data.include? key
          data[key] = data[key].concat values
        else
          data[key] = values.dup
        end
      end
      nil
    end

    def add_to_data_no_merge_double(data, keys, values)
      keys.each do |key|
        next if data.include? key
        data[key] = values 
      end
      nil
    end

    def add_to_data_merge(data, keys, values)
      keys.uniq.each do |key|
        if data.include? key
          new = data[key]
          new.each_with_index do |old, i|
            old.concat values[i]
          end
          data[key] = new
        else
          data[key] = values
        end
      end
      nil
    end

    def add_to_data_merge_zipped(data, keys, values)
      num = keys.length

      if values.first.length > 1 and num == 1
        keys = keys * values.first.length
        num = keys.length
      end

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
      nil
    end

    def add_to_data_zipped(data, keys, values)
      num = keys.length

      if values.first.length > 1 and num == 1
        keys = keys * values.first.length
        num = keys.length
      end

      values = values.collect{|v| v.length != num ? [v.first] * num : v}
      all = values.unshift keys
      Misc.zip_fields(all).each do |values|
        key = values.shift
        next if data.include? key
        data[key] = values.collect{|v| [v]}
      end
      nil
    end


    def cast_values_single(value)
      case
      when (value.nil? or value.empty?)
        nil
      when Symbol === cast
        value.send(cast)
      when Proc === cast
        cast.call value
      end
    end

    def cast_values_list(values)
      case
      when Symbol === cast
        values.collect{|v| v.nil? or v.empty? ? nil : v.send(cast)}
      when Proc === cast
        values.collect{|v| v.nil? or v.empty? ? nil : cast.call(v)}
      end
    end

    def cast_values_flat(values)
      case
      when Symbol === cast
        values.collect{|v| v.nil? or v.empty? ? nil : v.send(cast)}
      when Proc === cast
        values.collect{|v| v.nil? or v.empty? ? nil : cast.call(v) }
      end
    end

    def cast_values_double(values)
      case
      when Symbol === cast
        values.collect{|list| list.collect{|v| v.nil? or v.empty? ? nil : v.send(cast)}}
      when Proc === cast
        values.collect{|list| list.collect{|v| v.nil? or v.empty? ? nil : cast.call(v) }}
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
        when (key_field.nil? or (not Integer === key_field and @key_field.nil?) or key_field == @key_field or key_field == 0)
          @key_position = 0
        when Integer === key_field
          @key_position = key_field
        when String === key_field
          @key_position = @fields.dup.unshift(@key_field).index key_field
          raise "Key field #{ key_field } was not found" if @key_position.nil?
        when :key == key_field
          @key_position = 0
        else
          raise "Format of key_field not understood: #{key_field.inspect}"
        end

        if (fields.nil? or (not (Array === fields and Integer === fields.first) and @fields.nil?) or fields == @fields or (not @fields.nil? and fields == (1..@fields.length).to_a))
          if not @fields.nil? and type != :flat
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
              raise "Format of fields not understood: #{field.inspect}"
            end
          }
        end

        new_key_field = @fields.dup.unshift(@key_field)[@key_position] if not @fields.nil?
        @fields = @fields.dup.unshift(@key_field).values_at *@field_positions if not @fields.nil? and not @field_positions.nil?
        @fields ||= fields if Array === fields and String === fields.first
        @fields = [@key_field] if new_key_field != @key_field and type == :flat and @field_positions.nil?
        @key_field = new_key_field 
        @key_field ||= key_field if String === key_field

      end
    end

    def initialize(stream = nil, options = {})
      @header_hash = Misc.process_options(options, :header_hash) || "#"
      @sep = Misc.process_options(options, :sep) || "\t"
      @tsv_grep = Misc.process_options(options, :tsv_grep) 
      stream = TSV.get_stream stream
      @stream = stream


      header_options = parse_header(stream)

      options = header_options.merge options

      @type ||= Misc.process_options(options, :type) || :double
      @type ||= :double

      @identifiers = Misc.process_options(options, :identifiers) 

      @filename = Misc.process_options(options, :filename) 
      @filename ||= stream.filename if stream.respond_to? :filename

      @sep2 = Misc.process_options(options, :sep2) || "|"
      @cast = Misc.process_options options, :cast; @cast = @cast.to_sym if String === @cast
      @type ||= Misc.process_options options, :type
      @fix = Misc.process_options(options, :fix) 
      @select= Misc.process_options options, :select
      @zipped = Misc.process_options options, :zipped
      @namespace = Misc.process_options options, :namespace
      merge = Misc.process_options(options, :merge)
      merge = @zipped if merge.nil?
      merge = false if merge.nil?

      fields = options[:fields]
      fix_fields(options)

      @type = @type.strip.to_sym if String === @type
      #@type ||= :double if merge == true

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
        if header_options[:type] == :flat
          self.instance_eval do alias get_values get_values_single_from_flat end
          self.instance_eval do alias cast_values cast_values_single end
          self.instance_eval do alias add_to_data add_to_data_no_merge_double end
        else
          self.instance_eval do alias get_values get_values_single end
          self.instance_eval do alias cast_values cast_values_single end
          self.instance_eval do alias add_to_data add_to_data_no_merge_list end
        end
      when :list
        self.instance_eval do alias get_values get_values_list end
        self.instance_eval do alias cast_values cast_values_list end
        self.instance_eval do alias add_to_data add_to_data_no_merge_list end
      when :flat
        @take_all = true if field_positions.nil?
        self.instance_eval do alias cast_values cast_values_flat end
        merge = true if key_position and key_position != 0 and field_positions.nil?
        if merge 
          self.instance_eval do alias get_values get_values_flat_merge end
          if key_position and key_position != 0 and field_positions.nil?
            self.instance_eval do alias add_to_data add_to_data_flat_merge_keys end
          else
            self.instance_eval do alias add_to_data add_to_data_flat_merge_double end
          end
        else
          self.instance_eval do alias get_values get_values_flat end
          if key_position and key_position != 0 and field_positions.nil?
            self.instance_eval do alias add_to_data add_to_data_flat_keys end
          else
            self.instance_eval do alias add_to_data add_to_data_flat end
          end
        end
      else
        raise "Unknown TSV type: #{@type.inspect}"
      end


      @straight = false if @sep != "\t" or not @cast.nil? or merge or (@type == :flat and fields)
    end

    def setup(data)
      data.extend TSV unless TSV === data
      data.type = @type
      data.key_field = @key_field
      data.fields = @fields
      data.namespace = @namespace
      data.filename = @filename
      data.identifiers = @identifiers
      data.cast = @cast if Symbol === @cast
      data
    end

    def annotate(data)
      setup(data)
    end

    def options
      options = {}
      TSV::ENTRIES.each do |entry|
        if self.respond_to? entry
          value = self.send(entry) 
          options[entry.to_sym] = value unless value.nil?
        end
      end
      options[:sep] = @sep if @sep and @sep != "\t" and @sep != /\t/
      IndiferentHash.setup options
    end

    def traverse(options = {})
      monitor, grep, invert_grep, head = Misc.process_options options, :monitor, :grep, :invert_grep, :head
      raise "No block given in TSV::Parser#traverse" unless block_given?

      stream = @stream


      # first line
      line = self.rescue_first_line

      if @tsv_grep

        stream = Open.grep(stream, @tsv_grep, invert_grep)
        stream.no_fail = true
        begin
          match = Open.grep(StringIO.new(line), @tsv_grep, invert_grep).read
          line = stream.gets if match.empty?
        rescue Exception
          Log.exception $!
          line = stream.gets 
        end
      end

      progress_monitor, monitor = monitor, nil if Log::ProgressBar === monitor
      # setup monitor
      if monitor and (stream.respond_to?(:size) or (stream.respond_to?(:stat) and stream.stat.respond_to? :size and stream.respond_to?(:pos)))
        size = case
               when stream.respond_to?(:size)
                 stream.size
               else
                 stream.stat.size
               end
        size = nil if size.to_i == 0
        desc = "Parsing Stream"
        step = 100
        if Hash === monitor
          desc = monitor[:desc] if monitor.include? :desc 
          step = monitor[:step] if monitor.include? :step 
        end
        progress_monitor = Log::ProgressBar.new_bar(size, :desc => desc, :bytes => true)
      elsif progress_monitor 

        size = case
               when stream.respond_to?(:size)
                 stream.size
               else
                 stream.stat.size
               end

        progress_monitor.bytes = true
        progress_monitor.max =  size unless size.to_i == 0
      end

      # parser 
      line_num = 1
      begin

        while not line.nil? 
          begin
            if progress_monitor
              progress_monitor.tick(line.bytesize)
            end

            raise SKIP_LINE if line.empty?

            line = Misc.fixutf8(line)
            line = self.process line
            raise SKIP_LINE if line.empty?
            parts = self.chop_line line
            key, values = self.get_values parts
            values = self.cast_values values if self.cast?
            
            yield key, values

            line = stream.gets

            line_num += 1
            raise END_PARSING if head and line_num > head.to_i
          rescue SKIP_LINE
            begin
              line = stream.gets
              next
            rescue IOError
              break
            end
          rescue END_PARSING
            stream.close unless stream.closed?
            begin stream.join; rescue Exception; end if stream.respond_to? :join and not stream.joined?
            break
          rescue Errno::EPIPE
            Log.error "Pipe closed while parsing #{Misc.fingerprint stream}: #{$!.message}"
            stream.abort if stream.respond_to? :abort
            raise $!
          rescue Exception
            Log.error "Exception parsing #{Misc.fingerprint stream}: #{$!.message}"
            stream.abort $! if stream.respond_to? :abort
            raise $!
          end
        end
      ensure
        Log::ProgressBar.remove_bar(progress_monitor)
        stream.close unless stream.closed?
        stream.join if stream.respond_to? :join and not stream.joined?
      end

      self
    end

    def identify_field(field)
      TSV.identify_field(key_field, fields, field)
    end

    def self.traverse(stream, options = {}, &block)
      parser = Parser.new(stream, options)
      parser.traverse(options, &block)
    end
  end
end
