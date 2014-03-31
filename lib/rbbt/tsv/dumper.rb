module TSV
  class Dumper
    attr_accessor :in_stream, :stream, :options, :filename
    def self.stream(options = {}, filename = nil, &block)
      dumper = TSV::Dumper.new options, filename
      Thread.new do
        yield dumper
        dumper.close
      end
      dumper.stream
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
      @stream, @in_stream = IO.pipe
    end

    def self.values_to_s(values, fields = nil)
      case values
      when nil
        if fields.nil? or fields.empty?
          "\n"
        else
          "\t" << ([""] * fields.length) * "\t" << "\n"
        end
      when Array
        "\t" << values.collect{|v| Array === v ? v * "|" : v} * "\t" << "\n"
      else
        "\t" << values.to_s << "\n"
      end
    end

    def init
      options = @options.dup
      key_field, fields = Misc.process_options options, :key_field, :fields

      str = TSV.header_lines(key_field, fields, options)
      @in_stream.puts str
    end

    def add(k,v)
      @in_stream << k << TSV::Dumper.values_to_s(v, @options[:fields])
    end

    def close
      @in_stream.close
    end
  end
end
