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
      fields.index field
    end
  end

  def identify_field(field)
    TSV.identify_field(key_field, fields, field)
  end


end
