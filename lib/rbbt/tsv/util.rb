require 'rbbt/resource/path'
module TSV

  def self.stream_column(file, column)
    header = TSV.parse_header(file)
    pos = header.fields.index(column) + 1
    sep2 = header.options[:sep2] || "|"
    case header.type.to_s
    when nil, "double"
      TSV.traverse file, :type => :array, :into => :stream do |line|
        next if line =~ /^#/
        line.split("\t")[pos].gsub(sep2, "\n")
      end
    when "single"
      TSV.traverse file, :type => :array, :into => :stream do |line|
        next if line =~ /^#/
        line.split("\t")[1]
      end
    when "flat"
      TSV.traverse file, :type => :array, :into => :stream do |line|
        next if line =~ /^#/
        line.split("\t")[1..-1] * "\n"
      end
    when 'list'
      TSV.traverse file, :type => :array, :into => :stream do |line|
        next if line =~ /^#/
        line.split("\t")[pos]
      end
    end
  end

  def self.guess_id(identifier_file, values, options = {})
    field_matches = TSV.field_match_counts(identifier_file, values, options)
    field_matches.sort_by{|field, count| count.to_i}.last
  end

  def self.field_match_counts(file, values, options = {})
    options = Misc.add_defaults options, :persist_prefix => "Field_Matches"
    persist_options = Misc.pull_keys options, :persist

    filename = TSV === file ? file.filename : file
    path = Persist.persist filename, :string, persist_options.merge(:no_load => true) do
      tsv = TSV === file ? file : TSV.open(file, options)

      text = ""
      fields = nil
      tsv.tap{|e| e.unnamed =  true; fields = e.fields}.through do |gene, names|
        names.zip(fields).each do |list, format|
          list = [list] unless Array === list
          list.delete_if do |name| name.empty? end
          next if list.empty?
          text << list.collect{|name| [name, format] * "\t"} * "\n" << "\n"
        end
        text << [gene, tsv.key_field] * "\t" << "\n"
      end
      text
    end

    TmpFile.with_file(values.uniq * "\n", false) do |value_file|
      cmd = "cat '#{ path }' | sed 's/\\t/\\tHEADERNOMATCH/' | grep -w -F -f '#{ value_file }' | sed 's/HEADERNOMATCH//' |sort -u|cut -f 2  |sort|uniq -c|sed 's/^ *//;s/ /\t/'"
      begin
        TSV.open(CMD.cmd(cmd), :key_field => 1, :fields => [0], :type => :single, :cast => :to_i)
      rescue
        Log.exception $!
        TSV.setup({}, :type => :single, :cast => :to_i)
      end
    end
  end

  def self.get_filename(file)
    case
    when (defined? Step and Step === file)
      file.path
    when Path === file
      file
    when (String === file and (Open.exists? file or Open.remote? file))
      file
    when String === file 
      "String-#{Misc.digest file}"
    when file.respond_to?(:filename)
      file.filename
    when file.respond_to?(:gets)
      nil
    else
      raise "Cannot get filename from: #{file.inspect}"
    end
  end

  def self.abort_stream(file, exception = nil)
    return if file.nil?
    if defined? Step and Step === file
      if exception
        file.exception exception 
      else
        if not (file.aborted? or file.done?)
          file.abort 
        end
      end
    elsif Hash === file or Array === file
      return
    else
      stream = get_stream(file)
      stream.abort(exception) if stream.respond_to? :abort
      AbortedStream.setup(stream, exception) unless stream.respond_to?(:exception) && stream.exception
    end
  end

  def self.get_stream(file, open_options = {})
    case file
    when Zlib::GzipReader
      file
    when (defined? Bgzf and Bgzf)
      file
    when TSV
      file.dumper_stream
    when TSV::Dumper
      file.stream
    when TSV::Parser
      file.stream
    when Path
      file.open(open_options)
    when (defined? Tempfile and Tempfile)
      begin
        pos = file.pos
        file.rewind if file.respond_to?(:rewind) and pos != 0
      rescue Exception
      end
      file
    when IO, StringIO, File
      begin
        pos = file.pos
        file.rewind if file.respond_to?(:rewind) and pos != 0
      rescue
      end
      file
    when String
      if Open.remote?(file) || Open.ssh?(file) || Open.exist?(file) 
        Open.open(file, open_options)
      else
        StringIO.new file
      end
    when (defined? Step and Step)
      if file.respond_to?(:base_url) 
        if file.result and IO === file.result
          file.result
        else
          file.join
          get_stream(file.path, open_options.merge(:nocache => true))
        end
      else
        file.grace

        stream = file.get_stream
        if stream && ! stream.closed?
          stream
        else
          file.join
          raise "Aborted stream from Step #{file.path}" if file.aborted?
          raise "Exception in stream from Step #{file.path}: #{file.messages.last}" if file.error?
          get_stream(file.path, open_options)
        end
      end
    when Array
      Misc.open_pipe do |sin|
        file.each do |l|
          sin.puts l
        end
      end
    when Set
      get_stream(file.to_a, open_options)
    when Enumerable
      file
    else
      raise "Cannot get stream from: #{file.inspect}"
    end
  end

  def self.identify_field(key_field, fields, field)
    case field
    when nil
      :key
    when Symbol
      field == :key ? field : identify_field(key_field, fields, field.to_s)
    when Integer
      field
    when (fields.nil? and String)
      raise "No field information available and specified field not numeric: #{ field }" unless field =~ /^\d+$/
      identify_field(key_field, fields, field.to_i)
    when String
      return :key if key_field == field
      pos = fields.index field
      return pos if pos
      return identify_field(key_field, fields, field.to_i) if field =~ /^\d+$/
      if fields.select{|f| f.include?("(") }.any?
        simplify_fields = fields.collect do |f|
          if m = f.match(/(.*)\s+\(.*\)/)
            m[1]
          else
            f
          end
        end
        return identify_field(key_field, simplify_fields, field)
      end
      raise "Field '#{ field }' was not found. Options: (#{key_field || "NO_KEY_FIELD"}), #{(fields || ["NO_FIELDS"]) * ", "}" if pos.nil?
    else
      raise "Field '#{ field }' was not found. Options: (#{key_field || "NO_KEY_FIELD"}), #{(fields || ["NO_FIELDS"]) * ", "}"
    end
  end


  
  def self.header_lines(key_field, fields, entry_hash = nil)
    if Hash === entry_hash 
      sep = entry_hash[:sep] ? entry_hash[:sep] : "\t"
      preamble = entry_hash[:preamble]
      header_hash = entry_hash[:header_hash]
    end

    header_hash = "#" if header_hash.nil?

    preamble = "#: " << Misc.hash2string(entry_hash.merge(:key_field => nil, :fields => nil)) << "\n" if preamble.nil? and entry_hash and entry_hash.values.compact.any?

    str = "" 
    str << preamble.strip << "\n" if preamble and not preamble.empty?
    if fields
      if fields.empty?
        str << header_hash << (key_field || "ID").to_s << "\n" 
      else
        str << header_hash << (key_field || "ID").to_s << sep << (fields * sep) << "\n" 
      end
    end

    str
  end

  def identify_field(field)
    TSV.identify_field(key_field, fields, field)
  end

  def rename_field(field, new)
    self.fields = self.fields.collect{|f| f == field ? new : f }
    self
  end

  def unzip_replicates
    raise "Can only unzip replicates in :double TSVs" unless type == :double

    new = {}
    self.with_unnamed do
      through do |k,vs|
        Misc.zip_fields(vs).each_with_index do |v,i|
          new[k + "(#{i})"] = v
        end
      end
    end

    self.annotate(new)
    new.type = :list

    new
  end

  def to_list(&block)
    new = {}
    case type
    when :double
      if block_given?
        through do |k,v|
          if block.arity == 1
            new[k] = v.collect{|e| yield e}
          else
            new[k] = yield k, v
          end
        end
      else
        through do |k,v|
          new[k] = v.collect{|e| e.first}
        end
      end
    when :flat
      through do |k,v|
        new[k] = [v.first]
      end
    when :single
      through do |k,v|
        new[k] = [v]
      end
    when :list
      return self
    end
    self.annotate(new)
    new.type = :list
    new
  end

  def to_double
    new = {}
    case type
    when :double
      return self
    when :flat
      through do |k,v|
        new[k] = v.nil? ? [] : [v]
      end
    when :single
      through do |k,v|
        new[k] = v.nil? ? [[]] : [[v]]
      end
    when :list
      if block_given?
        through do |k,v|
          if v.nil?
            new[k] = nil
          else
            new[k] = v.collect{|e| yield e}
          end
        end
      else
        through do |k,v|
          if v.nil?
            new[k] = nil
          else
            new[k] = v.collect{|e| [e]}
          end
        end
      end
    end
    self.annotate(new)
    new.type = :double
    new
  end

  def to_flat(field = nil)
    new = {}
    case type
    when :double
      if field.nil?
        through do |k,v| new[k] = v.first end
      elsif field == :all
        through do |k,v| new[k] = v.flatten.compact end
      else
        pos = identify_field field
        through do |k,v| new[k] = v[pos] end
      end
    when :flat
      return self
    when :single
      through do |k,v|
        new[k] = [v]
      end
    when :list
      through do |k,v|
        new[k] = [v.first]
      end
    end
    self.annotate(new)
    if new.fields
      case field
      when nil
        new.fields = new.fields[0..0]
      when :all
        new.fields = [new.fields * "+"]
      else
        new.fields = [field]
      end
    end
    new.type = :flat
    new
  end

  def to_single
    new = {}

    if block_given?
      through do |k,v|
        new[k] = yield v
      end
    else
      case type
      when :double
        through do |k,v|
          new[k] = v.first.first
        end
      when :flat
        through do |k,v|
          new[k] = v.first
        end
      when :single
        return self
      when :list
        through do |k,v|
          new[k] = v.nil? ? nil : v.first
        end
      end
    end

    self.annotate(new)
    new.type = :single
    new.fields = [new.fields.first] if new.fields.length > 1
    new
  end


  def to_onehot(boolean = false)
    all_values = values.flatten.uniq.collect{|v| v.to_s}.sort
    index = TSV.setup({}, :key_field => key_field, :fields => all_values, :type => :list)
    index.cast = :to_i unless boolean
    through do |key,values|
      v = all_values.collect{|_v| values.include?(_v)}
      v = v.collect{|_v| _v ? 1 : 0 } unless boolean
      index[key] = v
    end
    index
  end

  def merge(other)
    self.annotate(super(other))
  end
end

