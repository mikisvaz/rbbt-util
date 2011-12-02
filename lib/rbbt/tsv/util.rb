require 'rbbt/resource/path'
module TSV

  def self.field_match_counts(file, values)
    fields = TSV.parse_header(Open.open(file)).all_fields

    counts = {}
    TmpFile.with_file do |tmpfile|
      if Array === values
        Open.write(tmpfile, values * "\n")
      else
        FileUtils.ln_s values, tmpfile
      end

      fields.each_with_index do |field,i|
        counts[field] = begin
                          CMD.cmd("cat #{ file } |grep -v ^#|cut -f #{i + 1}|tr '|' '\\n' |sort -u |grep [[:alpha:]]|grep -f #{tmpfile} -F -w").read.count("\n")
                        rescue
                          0
                        end
      end
    end

    counts
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
    TmpFile.with_file(values * "\n") do |value_file|
      cmd = "cat '#{ path }' | grep -w -F -f '#{ value_file }' |cut -f 2 |sort|uniq -c|sed 's/^ *//;s/ /\\t/'"
      begin
        TSV.open(CMD.cmd(cmd), :key_field => 1, :type => :single, :cast => :to_i)
      rescue
        TSV.setup({nil => 0}, :type => :single, :cast => :to_i)
      end
    end
  end

  def self.get_filename(file)
    case
    when String === file
      filename = file
    when file.respond_to?(:gets)
      filename = file.filename if file.respond_to? :filename
    else
      raise "Cannot get stream from: #{file.inspect}"
    end
    filename
  end

  def self.get_stream(file)
    case
    when Path === file
      file.open
    when String === file
      File.open(file)
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
      pos = fields.index field
      Log.medium "Field #{ field } was not found. Options: #{fields * ", "}" if pos.nil?
      pos
    end
  end

  def identify_field(field)
    TSV.identify_field(key_field, fields, field)
  end


end
