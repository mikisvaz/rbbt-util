require 'rbbt/util/resource'

module Resource
  module Path

    def tsv(type = nil, options = {})
      if options.empty? and Hash === type
        options, type = type, nil
      end

      TSV.new self.find, type, options
    end

    def index(options = {})
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

    def identifier_files
      dir = self.find.sub(self,'')
      if dir.nil? or dir.empty?
        path = File.join(File.dirname(self.find), 'identifiers')
        path.extend Path
        path.pkg_module = pkg_module
        if path.exists?
          [path]
        else
          []
        end
      else
        identifier_files = Misc.find_files_back_to(self.find, 'identifiers', dir)
        return identifier_files.collect{|f| Resource::Path.path(f)}
      end
    end
  end
end
