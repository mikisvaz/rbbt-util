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
    when (field.nil? or field == :key or key_field == field)
      :key
    when String === field
      raise "No fields specified in TSV.identify_field" if fields.nil?
      pos = fields.index field
      Log.medium "Field #{ field } was not found. Options: #{fields * ", "}" if pos.nil?
      pos
    end
  end

  def identify_field(field)
    TSV.identify_field(key_field, fields, field)
  end


end
