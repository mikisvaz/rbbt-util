module TSV
  class IntegerSerializer
    def self.dump(i); [i].pack("l"); end
    def self.load(str); str.unpack("l").first; end
  end

  class FloatSerializer
    def self.dump(i); [i].pack("d"); end
    def self.load(str); str.unpack("d").first; end
  end

  class IntegerArraySerializer
    def self.dump(a); a.pack("l*"); end
    def self.load(str); str.unpack("l*"); end
  end

  class StringSerializer
    def self.dump(str); str.to_s; end
    def self.load(str); str; end
  end

  class StringArraySerializer
    def self.dump(array)
      array.collect{|a| a.to_s} * "\t"
    end

    def self.load(string)
      return [] if string.nil?
      string.split("\t", -1)
    end
  end

  class StringDoubleArraySerializer
    def self.dump(array)
      array.collect{|a| a.collect{|a| a.to_s} * "|"} * "\t"
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
    :marshal => Marshal,
    :single => StringSerializer,
    :string => StringSerializer,
    :list => StringArraySerializer,
    :flat => StringArraySerializer,
    :double => StringDoubleArraySerializer,
    :tsv => TSVSerializer,
    :marshal_tsv => TSVMarshalSerializer
  }

end
