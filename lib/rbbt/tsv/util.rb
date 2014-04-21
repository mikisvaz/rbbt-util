require 'rbbt/resource/path'
module TSV

  def self.reorder_stream(stream, positions, sep = "\t")
    Misc.open_pipe do |sin|
      line = stream.gets
      while line =~ /^#\:/
        sin.puts line
        line = stream.gets
      end
      while line  =~ /^#/
        if Hash === positions
          new = (0..line.split(sep).length-1).to_a
          positions.each do |k,v|
            new[k] = v
            new[v] = k
          end
          positions = new
        end
        sin.puts "#" + line.sub!(/^#/,'').strip.split(sep).values_at(*positions).compact * sep
        line = stream.gets
      end
      while line
        if Hash === positions
          new = (0..line.split(sep).length-1).to_a
          positions.each do |k,v|
            new[k] = v
            new[v] = k
          end
          positions = new
        end
        sin.puts line.strip.split(sep).values_at(*positions) * sep
        line = stream.gets
      end
    end
  end

  def self.field_match_counts(file, values, options = {})
    options = Misc.add_defaults options, :persist_prefix => "Field_Matches"
    persist_options = Misc.pull_keys options, :persist

    filename = TSV === file ? file.filename : file
    text = Persist.persist filename, :string, persist_options do
      tsv = TSV === file ? file : TSV.open(file)

      text = ""
      fields = nil
      tsv.tap{|e| e.unnamed =  true; fields = e.fields}.through do |gene, names|
        names.zip(fields).each do |list, format|
          list.delete_if do |name| name.empty? end
          next if list.empty?
          text << list.collect{|name| [name, format] * "\t"} * "\n" << "\n"
        end
      end
      text
    end

    path = Persist.persistence_path(filename, persist_options)
    TmpFile.with_file(values.uniq * "\n") do |value_file|
      cmd = "cat '#{ path }' | sed 's/\\t/\\tHEADERNOMATCH/' | grep -w -F -f '#{ value_file }' |cut -f 2 | sed 's/HEADERNOMATCH//' | sort|uniq -c|sed 's/^ *//;s/ /\t/'"
      begin
        TSV.open(CMD.cmd(cmd), :key_field => 1, :type => :single, :cast => :to_i)
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
    when String === file
      file
    when file.respond_to?(:filename)
      file.filename
    when file.respond_to?(:gets)
      nil
    else
      raise "Cannot get filename from: #{file.inspect}"
    end
  end

  def self.get_stream(file, open_options = {})
    case
    when Path === file
      file.open(open_options)
    when file.respond_to?(:gets)
      file.rewind if file.respond_to?(:rewind) and file.eof?
      file
    when String === file
      Open.open(file, open_options)
    else
      raise "Cannot get stream from: #{file.inspect}"
    end
  end

  def self.get_stream(file, open_options = {})
    case file
    when Path
      file.open(open_options)
    when IO, StringIO
      begin
        file.rewind if file.respond_to?(:rewind) and file.eof?
      rescue
      end
      file
    when String
      raise "Could not open file given by String: #{Misc.fingerprint file}" unless Open.remote?(file) or File.exists? file
      Open.open(file, open_options)
    when (defined? Step and Step)
      stream = file.get_stream
      stream || get_stream(file.join.path)
    when TSV::Dumper
      file.stream
    when Array
      Misc.open_pipe do |sin|
        file.each do |l|
          sin.puts l
        end
      end
    else
      raise "Cannot get stream from: #{file.inspect}"
    end
  end

  def self.identify_field(key_field, fields, field)
    case
    when Integer === field
      field
    when (field.nil? or field == :key)
      :key
    when (String === field and not fields.nil?)
      pos = fields.index field
      pos ||= :key if key_field == field
      Log.medium "Field #{ field } was not found. Options: #{fields * ", "}" if pos.nil?
      pos
    when key_field == field
      :key
    else
      raise "No fields specified in TSV.identify_field" if fields.nil?
      Log.medium "Field #{ field } was not found. Options: (#{key_field}), #{fields * ", "}"
    end
  end
  
  def self.header_lines(key_field, fields, entry_hash = {})
    sep = (Hash === entry_hash and entry_hash[:sep]) ? entry_hash[:sep] : "\t"

    str = "" 
    str << "#: " << Misc.hash2string(entry_hash.merge(:key_field => nil, :fields => nil)) << "\n" if entry_hash and entry_hash.any?
    if fields
      str << "#" << key_field << sep << fields * sep << "\n"
    end

    str
  end

  def identify_field(field)
    TSV.identify_field(key_field, fields, field)
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
      self
    when :flat
      through do |k,v|
        new[k] = [v]
      end
    when :single
      through do |k,v|
        new[k] = [[v]]
      end
    when :list
      through do |k,v|
        new[k] = v.collect{|e| [e]}
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
      self
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
