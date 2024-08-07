module TSV
  class Dumper
    attr_accessor :in_stream, :stream, :options, :filename, :sep
    def self.stream(options = {}, filename = nil, stream = nil, &block)
      dumper = TSV::Dumper.new options, filename
      if stream
        dumper.set_stream stream if stream
        yield dumper
        stream
      else
        thread = Thread.new(Thread.current) do |parent|
          yield dumper
        end
        ConcurrentStream.setup(dumper.stream, threads: thread)
      end
    end

    def set_stream(stream)
      @stream.close
      @in_stream.close
      @in_stream = @stream = stream
    end

    def initialize(options, filename = nil)
      if TSV  === options
        @options = options.options.merge(:key_field => options.key_field, :fields => options.fields)
        @filename ||= options.filename
      else
        @options = options
        @filename = filename
      end
      @filename ||= Misc.fingerprint options
      @stream, @in_stream = Misc.pipe
    end

    def self.values_to_s(values, fields = nil, sep = "\t")
      sep = "\t" if sep.nil?
      case values
      when nil
        if fields.nil? or fields.empty?
          "\n"
        else
          sep + ([""] * fields.length) * sep << "\n"
        end
      when Array
        if fields.nil? 
          sep + (values.collect{|v| Array === v ? v * "|" : v} * sep) << "\n"
        elsif fields.empty?
          "\n"
        else
          sep + (values.collect{|v| Array === v ? v * "|" : v} * sep) << "\n"
        end
      else
        if fields.nil?
          sep + values.to_s + "\n"
        elsif fields.empty?
          "\n"
        else
          sep + values.to_s << "\n"
        end
      end
    end

    def init(init_options = {})
      options = @options.dup
      key_field, fields = Misc.process_options options, :key_field, :fields

      str = TSV.header_lines(key_field, fields, options.merge(init_options || {}))

      Thread.pass while IO.select(nil, [@in_stream],nil,1).nil? if IO === @in_stream

      @in_stream << str
    end

    def add(k,v)
      @fields ||= @options[:fields]
      @sep ||= @options[:sep]
      begin
        Thread.pass while IO.select(nil, [@in_stream],nil,1).nil? if IO === @in_stream
        @in_stream << k << TSV::Dumper.values_to_s(v, @fields, @sep)
      rescue IOError
      rescue Exception
        raise $!
      end
    end

    def close_out
      @stream.close unless StringIO === @stream || @stream.closed?
    end

    def close_in
      @in_stream.join if @in_stream.respond_to?(:join) && ! @in_stream.joined?
      @in_stream.close if @in_stream.respond_to?(:close) && ! @in_stream.closed? 
    end

    def close
      close_in unless @in_stream == @stream
    end
  end
end
