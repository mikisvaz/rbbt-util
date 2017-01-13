require 'rbbt/tsv'
require 'rbbt/tsv/attach/util'
module TSV

  # Merge columns from different rows of a file
  def self.merge_row_fields(input, output, options = {})
    options = Misc.add_defaults options, :sep => "\t"
    key_field, fields = Misc.process_options options, :key_field, :fields
    sep = options[:sep]

    is = case
         when (String === input and not input.index("\n") and input.length < 250 and File.exist?(input))
           CMD.cmd("env LC_ALL=C sort -k1,1 -t'#{sep}' #{ input } | grep -v '^#{sep}' ", :pipe => true)
         when (String === input or StringIO === input)
           CMD.cmd("env LC_ALL=C sort -k1,1 -t'#{sep}' | grep -v '^#{sep}'", :in => input, :pipe => true)
         else
           input
         end

    if key_field.nil? or fields.nil?
      parser = TSV::Parser.new(is, options.dup)
      fields ||= parser.fields
      key_field ||= parser.key_field
      line = parser.first_line
    else
      line = is.gets
    end
 
    current_key  = nil
    current_parts = []

    done = false
    Open.write(output) do |os|
      options.delete :sep if options[:sep] == "\t"
      os.puts TSV.header_lines(key_field, fields, options) 

      while line
        key, *parts = line.sub("\n",'').split(sep, -1)
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

        line = is.gets
      end

      os.puts [current_key, current_parts].flatten * sep unless current_key.nil?

    end
  end

  # Merge two files with the same keys and different fields
  def self.merge_different_fields(file1, file2, output, options = {})
    options = Misc.add_defaults options, :sep => "\t"
    monitor, key_field, fields = Misc.process_options options, :monitor, :key_field, :fields
    sep = options[:sep] || "\t"

    case
    when (String === file1 and not file1 =~ /\n/ and file1.length < 250 and File.exist?(file1))
      size = CMD.cmd("wc -c '#{file1}'").read.to_f if monitor
      file1 = CMD.cmd("env LC_ALL=C sort -k1,1 -t'#{sep}' #{ file1 } | grep -v '^#{sep}' ", :pipe => true)
    when (String === file1 or StringIO === file1)
      size = file1.length if monitor
      file1 = CMD.cmd("env LC_ALL=C sort -k1,1 -t'#{sep}' | grep -v '^#{sep}'", :in => file1, :pipe => true)
    when TSV === file1
      size = file1.size if monitor
      file1 = CMD.cmd("env LC_ALL=C sort -k1,1 -t'#{sep}' | grep -v '^#{sep}'", :in => file1.to_s(:sort, true), :pipe => true)
    end

    case
    when (String === file2 and not file2 =~ /\n/ and file2.length < 250 and File.exist?(file2))
      file2 = CMD.cmd("env LC_ALL=C sort -k1,1 -t'#{sep}' #{ file2 } | grep -v '^#{sep}' ", :pipe => true)
    when (String === file2 or StringIO === file2)
      file2 = CMD.cmd("env LC_ALL=C sort -k1,1 -t'#{sep}' | grep -v '^#{sep}'", :in => file2, :pipe => true)
    when TSV === file2
      file2 = CMD.cmd("env LC_ALL=C sort -k1,1 -t'#{sep}' | grep -v '^#{sep}'", :in => file2.to_s(:sort, true), :pipe => true)
    end

    begin
      output = File.open(output, 'w') if String === output

      cols1 = nil
      cols2 = nil

      done1 = false
      done2 = false

      key1 = key2 = nil
      while key1.nil?
        while (line1 = file1.gets) =~ /^#/
          key_field1, *fields1 = line1.strip.sub('#','').split(sep)
        end
        key1, *parts1 = line1.sub("\n",'').split(sep, -1)
        cols1 = parts1.length
      end

      while key2.nil?
        while (line2 = file2.gets) =~ /^#/
          key_field2, *fields2 = line2.strip.sub('#','').split(sep)
        end
        key2, *parts2 = line2.sub("\n",'').split(sep, -1)
        cols2 = parts2.length
      end

      #progress_monitor = Progress::Bar.new(size, 0, 100, "Merging fields") if monitor
      progress_monitor = Log::ProgressBar.new(size, :desc => "Merging fields") if monitor

      entry_hash = options
      entry_hash.delete :sep if entry_hash[:sep] == "\t"
      output.puts TSV.header_lines key_field1, fields1 + fields2, entry_hash if key_field1 and fields1 and fields2

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
      file1.join if file1.respond_to? :join
      file2.join if file2.respond_to? :join
    rescue
      file1.abort if file1.respond_to? :abort
      file2.abort if file2.respond_to? :abort
      file1.join if file1.respond_to? :join
      file2.join if file2.respond_to? :join
    end
  end

  # Merge columns from different files
  def self.merge_paste(files, delim = "$")
    CMD.cmd("paste #{ files.collect{|f| "'#{f}'"} * " "} -d'#{delim}' |sed 's/#{delim}[^\\t]*//g'", :pipe => true)
  end

  def merge_different_fields(other, options = {})
    TmpFile.with_file do |output|
      TSV.merge_different_fields(self, other, output, options)
      tsv = TSV.open output, options
      tsv.key_field = self.key_field unless self.key_field.nil?
      tsv.fields = self.fields + other.fields unless self.fields.nil? or other.fields.nil?
      tsv
    end
  end

  def merge_zip(other)
    other.each do |k,v|
      self.zip_new k, v
    end
  end

  
  def attach(other, options = {})
    options      = Misc.add_defaults options, :in_namespace => false, :persist_input => false
    fields, one2one, complete = Misc.process_options options, :fields, :one2one, :complete
    in_namespace = options[:in_namespace]

    unless TSV === other
      other_identifier_file = other.identifier_files.first if other.respond_to? :identifier_files
      other = TSV.open(other, :persist => options[:persist_input].to_s == "true")
      other.identifiers ||= other_identifier_file
    end

    fields = other.fields - [key_field].concat(self.fields) if other.fields and (fields.nil? or fields == :all)
    if in_namespace
      fields = other.fields_in_namespace - [key_field].concat(self.fields) if fields.nil?
    else
      fields = other.fields - [key_field].concat(self.fields) if fields.nil?
    end

    other_filename = other.respond_to?(:filename) ? other.filename : other.inspect
    Log.low("Attaching fields:#{Misc.fingerprint fields } from #{other_filename}.")

    if complete
      fill = TrueClass === complete ? nil : complete
      field_length = self.fields.length 
      missing = other.keys - self.keys
      case type
      when :single
        missing.each do |k|
          self[k] = nil
        end
      when :list
        missing.each do |k|
          self[k] = [nil] * field_length
        end
      when :double
        missing.each do |k|
          self[k] = [[]] * field_length
        end
      when :flat
        missing.each do |k|
          self[k] = []
        end
      end
    end

    same_key = true
    begin
      case
      when (key_field == other.key_field and same_key)
        Log.debug "Attachment with same key: #{other.key_field}"
        attach_same_key other, fields
      when (not in_namespace and self.fields.include?(other.key_field))
        Log.debug "Found other key field: #{other.key_field}"
        attach_source_key other, other.key_field, :fields => fields, :one2one => one2one
      when (in_namespace and self.fields_in_namespace.include?(other.key_field))
        Log.debug "Found other key field in #{in_namespace}: #{other.key_field}"
        attach_source_key other, other.key_field, :fields => fields, :one2one => one2one
      else
        index = TSV.find_traversal(self, other, options)
        raise FieldNotFoundError, "Cannot traverse identifiers" if index.nil?
        Log.debug "Attachment with index: #{other.key_field}"
        attach_index other, index, fields
      end
    rescue Exception
      if same_key
        Log.warn "Could not translate identifiers with same_key"
        same_key = false
        retry
      else
        raise $!
      end
    end
    Log.debug("Attachment of fields:#{Misc.fingerprint fields } from #{other.filename.inspect} finished.")

    self
  end

  def detach(file)
    file_fields = file.fields.collect{|field| field.fullname}
    detached_fields = []
    self.fields.each_with_index{|field,i| detached_fields << i if file_fields.include? field.fullname}
    reorder :key, detached_fields
  end
 
end
