require 'rbbt/util/resource'

module Resource
  module Path
    def tsv(key = nil, options = {})
      if options.empty? and Hash === key
        options, key = key, nil
      end

      produce
      TSV.new self.find, key, options
    end

    def index(options = {})
      produce
      TSV.index self.find, options
    end

    def open(options = {})
      produce
      Open.open(self.find, options)
    end

    def read(options = {})
      produce
      Open.read(self.find, options)
    end

    def fields(sep = nil, header_hash = nil)
      produce
      TSV.parse_header(self.open, sep, header_hash)[1].collect{|f| f.extend TSV::Field; f.namespace = namespace ;f}
    end

    def all_fields(sep = nil, header_hash = nil)
      produce
      key_field, fields = TSV.parse_header(self.open, sep, header_hash).values_at(0, 1).flatten.collect{|f| f.extend TSV::Field; f.namespace = namespace; f}
    end

    def fields_in_namespace(sep = nil, header_hash = nil)
      produce
      TSV.parse_header(self.open, sep, header_hash)[1].collect{|f| f.extend TSV::Field; f.namespace = namespace ;f}.select{|f| f.namespace == namespace}
    end

    def all_namespace_fields(namespace, sep = /\t/, header_hash = "#")
      produce
      key_field, fields = TSV.parse_header(self.open, sep, header_hash).values_at(0, 1).flatten.collect{|f| f.extend TSV::Field; f.namespace = namespace; f}.select{|f| f.namespace == namespace}
    end
  end
end
