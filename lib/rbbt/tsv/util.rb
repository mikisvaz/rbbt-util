require 'rbbt/resource/path'
module TSV

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
        TSV.open(CMD.cmd(cmd), :key_field => 1, :type => :single, :cast => :to_i)
      rescue
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
      AbortedStream.setup(stream, exception)
    end
  end

  def self.get_stream(file, open_options = {})
    case file
    when Zlib::GzipReader
      file
    when (defined? Bgzf and Bgzf)
      file
    when TSV
      file
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
      if Open.remote?(file) or File.exist? file
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
        if stream
          stream
        else
          file.join
          raise "Aborted stream from Step #{file.path}" if file.aborted?
          raise "Exception in stream from Step #{file.path}: #{file.messages.last}" if file.error?
          get_stream(file.path)
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
      raise "Field #{ field } was not found. Options: (#{key_field || "NO_KEY_FIELD"}), #{(fields || ["NO_FIELDS"]) * ", "}" if pos.nil?
    else
      raise "Field #{ field } was not found. Options: (#{key_field || "NO_KEY_FIELD"}), #{(fields || ["NO_FIELDS"]) * ", "}"
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
      str << header_hash << (key_field || "ID").to_s << sep << (fields * sep) << "\n" 
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

  def to_list
    new = {}
    case type
    when :double
      if block_given?
        through do |k,v|
          new[k] = v.collect{|e| yield e}
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
      self
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
    new.fields = new.fields[0..0] if new.fields
    new.type = :flat
    new
  end

  def to_single
    new = {}
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
      self
    when :list
      through do |k,v|
        new[k] = v.first
      end
    end
    self.annotate(new)
    new.type = :single
    new
  end

  def marshal_dump
    [info, to_hash]
  end
end

class Hash
  def marshal_load(array)
    info, to_hash = array
    self.merge! to_hash
    TSV.setup(self)
  end
end
