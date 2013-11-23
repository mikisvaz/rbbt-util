require 'rbbt/resource/path'
module TSV

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
        TSV.setup({}, :type => :single, :cast => :to_i)
      end
    end
  end

  def self.get_filename(file)
    case
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
    when String === file
      Open.open(file, open_options)
    when file.respond_to?(:gets)
      file
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

  def identify_field(field)
    TSV.identify_field(key_field, fields, field)
  end

  def to_list
    new = {}
    case type
    when :double
      through do |k,v|
        new[k] = v.collect{|e| e.first}
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

end
