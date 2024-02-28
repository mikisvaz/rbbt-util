require 'base64'
module TSV

  class CleanSerializer
    def self.dump(o); o end
    def self.load(o); o end
  end

  class BinarySerializer
    def self.dump(o); [o].pack('m'); end
    def self.load(str); str.unpack('m').first; end
  end
  
  class IntegerSerializer
    def self.dump(i); [i].pack("l"); end
    def self.load(str); str.unpack("l").first; end
  end

  class FloatSerializer
    def self.dump(i); [i].pack("d"); end
    def self.load(str); str.unpack("d").first; end
  end

  class StrictIntegerArraySerializer
    def self.dump(a); a.pack("l*"); end
    def self.load(str); a = str.unpack("l*"); end
  end

  class StrictFloatArraySerializer
    def self.dump(a); a.pack("d*"); end
    def self.load(str); a = str.unpack("d*"); end
  end

  class IntegerArraySerializer
    NIL_INT = -999
    def self.dump(a); a.collect{|v| v || NIL_INT}.pack("l*"); end
    def self.load(str); a = str.unpack("l*"); a.collect{|v| v == NIL_INT ? nil : v}; end
  end

  class FloatArraySerializer
    NIL_FLOAT = -999.999
    def self.dump(a); a.collect{|v| v || NIL_FLOAT}.pack("d*"); end
    def self.load(str); a = str.unpack("d*"); a.collect{|v| v == NIL_FLOAT ? nil : v}; end
  end

  class StringSerializer
    def self.dump(str); str.to_s; end
    def self.load(str); str.dup; end
  end

  class StringArraySerializer
    def self.dump(array)
      array.collect{|a| a.to_s} * "\t"
    end

    def self.load(string)
      return nil if string.nil? or string == 'nil'
      return [] if string.empty?
      string.split("\t", -1)
    end
  end

  class StringDoubleArraySerializer
    def self.dump(array)
      begin
        array.collect{|a| a.collect{|a| a.to_s } * "|"} * "\t"
      rescue Encoding::CompatibilityError
        array.collect{|a| a.collect{|a| a.to_s.force_encoding('UTF-8')} * "|"} * "\t"
      end
    end

    def self.load(string)
      return [] if string.nil?
      string.split("\t", -1).collect{|l| l.split("|", -1)}
    end
  end

  class TSVMarshalSerializer
    def self.dump(tsv)
      Marshal.dump(tsv.dup)
    end

    def self.load(string)
      TSV.setup Marshal.load(string)
    end
  end

  class TSVSerializer
    def self.dump(tsv)
      tsv.to_s
    end

    def self.load(string)
      TSV.open StringIO.new(string)
    end
  end

  SERIALIZER_ALIAS = {
    :integer => IntegerSerializer, 
    :float => FloatSerializer, 
    :integer_array => IntegerArraySerializer,
    :float_array => FloatArraySerializer,
    :strict_integer_array => StrictIntegerArraySerializer,
    :strict_float_array => StrictFloatArraySerializer,
    :marshal => Marshal,
    :single => StringSerializer,
    :string => StringSerializer,
    :list => StringArraySerializer,
    :flat => StringArraySerializer,
    :double => StringDoubleArraySerializer,
    :clean => CleanSerializer,
    :binary => BinarySerializer,
    :tsv => TSVSerializer,
    :marshal_tsv => TSVMarshalSerializer
  }

end
