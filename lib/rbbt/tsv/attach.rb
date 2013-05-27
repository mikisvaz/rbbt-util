require 'rbbt/tsv'
require 'rbbt/tsv/attach/util'
module TSV

  # Merge columns from different rows of a file
  def self.merge_row_fields(input, output, sep = "\t")
    is = case
         when (String === input and not input.index("\n") and input.length < 250 and File.exists?(input))
           CMD.cmd("sort -k1,1 -t'#{sep}' #{ input } | grep -v '^#{sep}' ", :pipe => true)
         when (String === input or StringIO === input)
           CMD.cmd("sort -k1,1 -t'#{sep}' | grep -v '^#{sep}'", :in => input, :pipe => true)
         else
           input
         end
 
    current_key  = nil
    current_parts = []

    done = false
    Open.write(output) do |os|

      done = is.eof?
      while not done
        key, *parts = is.gets.sub("\n",'').split(sep, -1)
        current_key ||= key
        case
        when key.nil?
        when current_key == key
          parts.each_with_index do |part,i|
            if current_parts[i].nil?
              current_parts[i] = part
            else
              current_parts[i] = current_parts[i] << "|" << part
            end
          end
        when current_key != key
          os.puts [current_key, current_parts].flatten * sep
          current_key = key
          current_parts = parts
        end

        done = is.eof?
      end

      os.puts [current_key, current_parts].flatten * sep unless current_key.nil?

    end
  end

  # Merge two files with the same keys and different fields
  def self.merge_different_fields(file1, file2, output, sep = "\t", monitor = false)
    case
    when (String === file1 and not file1 =~ /\n/ and file1.length < 250 and File.exists?(file1))
      size = CMD.cmd("wc -l '#{file1}'").read.to_f if monitor
      file1 = CMD.cmd("sort -k1,1 -t'#{sep}' #{ file1 } | grep -v '^#{sep}' ", :pipe => true)
    when (String === file1 or StringIO === file1)
      size = file1.length if monitor
      file1 = CMD.cmd("sort -k1,1 -t'#{sep}' | grep -v '^#{sep}'", :in => file1, :pipe => true)
    when TSV === file1
      size = file1.size if monitor
      file1 = CMD.cmd("sort -k1,1 -t'#{sep}' | grep -v '^#{sep}'", :in => file1.to_s(:sort, true), :pipe => true)
    end

    case
    when (String === file2 and not file2 =~ /\n/ and file2.length < 250 and File.exists?(file2))
      file2 = CMD.cmd("sort -k1,1 -t'#{sep}' #{ file2 } | grep -v '^#{sep}' ", :pipe => true)
    when (String === file2 or StringIO === file2)
      file2 = CMD.cmd("sort -k1,1 -t'#{sep}' | grep -v '^#{sep}'", :in => file2, :pipe => true)
    when TSV === file2
      file2 = CMD.cmd("sort -k1,1 -t'#{sep}' | grep -v '^#{sep}'", :in => file2.to_s(:sort, true), :pipe => true)
    end

    output = File.open(output, 'w') if String === output

    cols1 = nil
    cols2 = nil

    done1 = false
    done2 = false

    key1 = key2 = nil
    while key1.nil?
      while (line1 = file1.gets) =~ /#/; end
      key1, *parts1 = line1.sub("\n",'').split(sep, -1)
      cols1 = parts1.length
    end

    while key2.nil?
      while (line2 = file2.gets) =~ /#/; end
      key2, *parts2 = line2.sub("\n",'').split(sep, -1)
      cols2 = parts2.length
    end

    progress_monitor = Progress::Bar.new(size, 0, 100, "Merging fields") if monitor

    key = key1 < key2 ? key1 : key2
    parts = [""] * (cols1 + cols2)
    while not (done1 and done2)
      while (not done1 and key1 == key)
        parts1.each_with_index do |part, i|
          parts[i] = (parts[i].nil? or parts[i].empty?) ? part : parts[i] << "|" << part
        end
        key1 = nil
        while key1.nil? and not done1
          if file1.eof?; done1 = true; else key1, *parts1 = file1.gets.sub("\n",'').split(sep, -1) end
        end
        progress_monitor.tick if monitor
      end
      while (not done2 and key2 == key)
        parts2.each_with_index do |part, i|
          i += cols1
          parts[i] = (parts[i].nil? or parts[i].empty?) ? part : parts[i] << "|" << part
        end
        key2 = nil
        while key2.nil? and not done2
          if file2.eof?; done2 = true; else key2, *parts2 = file2.gets.sub("\n",'').split(sep, -1) end
        end
      end

      output.puts [key, parts].flatten * sep
      parts = [""] * (cols1 + cols2)

      case
      when done1
        key = key2
      when done2
        key = key1
      else
        key = key1 < key2 ? key1 : key2
      end
    end

    output.close
  end

  # Merge columns from different files
  def self.merge_paste(files, delim = "$")
    CMD.cmd("paste #{ files.collect{|f| "'#{f}'"} * " "} -d'#{delim}' |sed 's/#{delim}[^\\t]*//g'", :pipe => true)
  end

  def attach(other, options = {})
    options      = Misc.add_defaults options, :in_namespace => false, :persist_input => true
    fields, one2one = Misc.process_options options, :fields, :one2one
    in_namespace = options[:in_namespace]

    unless TSV === other
      other_identifier_files = other.identifier_files if other.respond_to? :identifier_files
      other = TSV.open(other, :persist => options[:persist_input] == true) unless TSV === other 
      other.identifiers = other_identifier_files 
    end

    fields = other.fields - [key_field].concat(self.fields) if fields.nil?  or fields == :all 
    if in_namespace
      fields = other.fields_in_namespace - [key_field].concat(self.fields) if fields.nil?
    else
      fields = other.fields - [key_field].concat(self.fields) if fields.nil?
    end

    other_filename = other.respond_to?(:filename) ? other.filename : other.inspect
    Log.low("Attaching fields:#{fields.inspect} from #{other_filename}.")

    case
    when key_field == other.key_field
      attach_same_key other, fields
    when (not in_namespace and self.fields.include?(other.key_field))
      Log.debug "Found other's key field: #{other.key_field}"
      attach_source_key other, other.key_field, :fields => fields, :one2one => one2one
    when (in_namespace and self.fields_in_namespace.include?(other.key_field))
      Log.debug "Found other's key field in #{in_namespace}: #{other.key_field}"
      attach_source_key other, other.key_field, :fields => fields, :one2one => one2one
    else
      index = TSV.find_traversal(self, other, options)
      raise "Cannot traverse identifiers" if index.nil?
      attach_index other, index, fields
    end
    Log.debug("Attachment of fields:#{fields.inspect} from #{other.filename.inspect} finished.")

    self
  end

  def detach(file)
    file_fields = file.fields.collect{|field| field.fullname}
    detached_fields = []
    self.fields.each_with_index{|field,i| detached_fields << i if file_fields.include? field.fullname}
    reorder :key, detached_fields
  end

  def merge_different_fields(other, options = {})
    TmpFile.with_file do |output|
      TSV.merge_different_fields(self, other, output, options[:sep] || "\t")
      tsv = TSV.open output, options
      tsv.key_field = self.key_field unless self.key_field.nil?
      tsv.fields = self.fields + other.fields unless self.fields.nil? or other.fields.nil?
      tsv
    end
  end
end
