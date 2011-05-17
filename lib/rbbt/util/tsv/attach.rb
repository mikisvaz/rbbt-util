class TSV
  def self.merge_rows(input, output, sep = "\t")
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

    end
  end

  def self.paste_merge(file1, file2, output, sep = "\t")
    case
    when (String === file1 and not file1.index("\n") and file1.length < 250 and File.exists?(file1))
      file1 = CMD.cmd("sort -k1,1 -t'#{sep}' #{ file1 } | grep -v '^#{sep}' ", :pipe => true)
    when (String === file1 or StringIO === file1)
      file1 = CMD.cmd("sort -k1,1 -t'#{sep}' | grep -v '^#{sep}'", :in => file1, :pipe => true)
    when TSV === file1
      file1 = CMD.cmd("sort -k1,1 -t'#{sep}' | grep -v '^#{sep}'", :in => file1.to_s(:sort, true), :pipe => true)
    end

    case
    when (String === file2 and not file2.index("\n") and file2.length < 250 and File.exists?(file2))
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
  #{{{ Attach Methods
  
  def attach_same_key(other, fields = nil)
    fields = other.fields - [key_field].concat(self.fields) if fields.nil?

    through do |key, values|
      if other.include? key
        new_values = other[key].values_at *fields
        new_values.collect!{|v| [v]}     if     type == :double and not other.type == :double
        new_values.collect!{|v| v.nil? ? nil : (other.type == :single ? v : v.first)} if not type == :double and     other.type == :double
        self[key] = self[key].concat new_values
      else
        if type == :double
          self[key] = self[key].concat [[]] * fields.length
        else
          self[key] = self[key].concat [""] * fields.length
        end
      end
    end

    self.fields = self.fields.concat other.fields.values_at *fields
  end

  def attach_source_key(other, source, fields = nil)
    fields = other.fields - [key_field].concat(self.fields) if fields.nil?

    other = other.tsv(:persistence => :no_create) unless TSV === other
    field_positions = fields.collect{|field| other.identify_field field}
    field_names     = field_positions.collect{|pos| pos == :key ? other.key_field : other.fields[pos] }

    through do |key, values|
      source_keys = values[source]
      source_keys = [source_keys] unless Array === source_keys
      if source_keys.nil? or source_keys.empty?
        all_new_values = []
      else
        all_new_values = []
        source_keys.each do |source_key|
          next unless other.include? source_key
          new_values = field_positions.collect do |pos|
            if pos == :key
              source_key
            else
              other[source_key][pos]
            end
          end

          new_values.collect!{|v| [v]}                                                  if     type == :double and not other.type == :double
          new_values.collect!{|v| v.nil? ? nil : (other.type == :single ? v : v.first)} if not type == :double and     other.type == :double
          all_new_values << new_values
        end
      end

      if all_new_values.empty?
        if type == :double
          self[key] = self[key].concat [[]] * field_positions.length
        else
          self[key] = self[key].concat [""] * field_positions.length
        end
      else
        if type == :double
          self[key] = self[key].concat TSV.zip_fields(all_new_values).collect{|l| l.flatten}
        else
          self[key] = self[key].concat all_new_values.first
        end
      end
    end

    self.fields = self.fields.concat field_names
  end

  def attach_index(other, index, fields = nil)
    fields = other.fields - [key_field].concat(self.fields) if fields.nil?
    fields = [fields] unless Array === fields

    other = other.tsv unless TSV === other
    field_positions = fields.collect{|field| other.identify_field field}
    field_names     = field_positions.collect{|pos| pos == :key ? other.key_field : other.fields[pos] }

    length = self.fields.length
    through do |key, values|
      source_keys = index[key]
      if source_keys.nil? or source_keys.empty?
        all_new_values = []
      else
        all_new_values = []
        source_keys.each do |source_key|
          next unless other.include? source_key
          new_values = field_positions.collect do |pos|
            if pos == :key
              if other.type == :double
                [source_key]
              else
                source_key
              end
            else
              other[source_key][pos]
            end
          end
          new_values.collect!{|v| v.nil? ? [[]] : [v]}    if     type == :double and not other.type == :double
          new_values.collect!{|v| v.nil? ? nil : (other.type == :single ? v : v.first)} if not type == :double and     other.type == :double
          all_new_values << new_values
        end
      end

      if all_new_values.empty?
        if type == :double
          all_new_values = [[[]] * field_positions.length]
        else
          all_new_values = [[""] * field_positions.length]
        end
      end

      current = self[key]

      if current.length > length
        all_new_values << current.slice!(length..current.length - 1)
      end

      if type == :double
        all_new_values = TSV.zip_fields(all_new_values).collect{|l| l.flatten}
      else
        all_new_values = all_new_values.first
      end
     
      current += all_new_values

      self[key] = current

    end

    self.fields = self.fields.concat field_names
  end

  #{{{ Attach Helper

  # May make an extra index!
  def self.find_path(files, options = {})
    options      = Misc.add_defaults options, :in_namespace => false
    in_namespace = options[:in_namespace]

    if in_namespace
      if files.first.all_fields.include? in_namespace
        ids = [[in_namespace]]
      else
        ids = [files.first.all_namespace_fields(in_namespace)]
      end
      ids += files[1..-1].collect{|f| f.all_fields}
    else
      ids = files.collect{|f| f.all_fields}
    end
    id_list = []

    ids.each_with_index do |list, i|
      break if i == ids.length - 1
      match = list.select{|field| 
        ids[i + 1].select{|f| field == f}.any?
      }
      return nil if match.empty?
      id_list << match.first
    end
    
    if id_list.last != files.last.all_fields.first
      id_list << files.last.all_fields.first
      id_list.zip(files)
    else
      id_list.zip(files[0..-1])
    end
  end

  def self.build_traverse_index(files, options = {})
    options       = Misc.add_defaults options, :in_namespace => false, :persist_input => false
    in_namespace  = options[:in_namespace]
    persist_input = options[:persist_input]

    path = find_path(files, options)

    return nil if path.nil?
    
    traversal_ids = path.collect{|p| p.first}

    Log.medium "Found Traversal: #{traversal_ids * " => "}"
    
    Persistence.persist(traversal_ids * "->", "Traversal", :tsv, :persistence => (persist_input and (data_key == data_file.key_field))) do
      data_key, data_file = path.shift
      data_index = if data_key == data_file.key_field
                     Log.debug "Data index not required '#{data_file.key_field}' => '#{data_key}'"
                     nil
                   else
                     Log.debug "Data index required"
                     data_file.index :target => data_key, :fields => data_file.key_field, :persistence => false
                   end

      current_index = data_index
      current_key   = data_key
      while not path.empty?
        next_key, next_file = path.shift

        if current_index.nil?
          current_index = next_file.index :target => next_key, :fields => current_key, :persistence => false
        else
          next_index = next_file.index :target => next_key, :fields => current_key, :persistence => false
          current_index.process current_index.fields.first do |values|
            if values.nil?
              nil
            else
              next_index.values_at(*values).flatten.collect.to_a
            end
          end
          current_index.fields = [next_key]
        end
        current_key = next_key
      end

      current_index
    end
  end


  def self.find_traversal(tsv1, tsv2, options = {})
    options      = Misc.add_defaults options, :in_namespace => false
    in_namespace = options[:in_namespace]

    identifiers1 = tsv1.identifier_files || []
    identifiers2 = tsv2.identifier_files || []

    identifiers1.unshift tsv1
    identifiers2.unshift tsv2

    files1 = []
    files2 = []
    while identifiers1.any?
      files1.push identifiers1.shift
      identifiers2.each_with_index do |e,i|
        files2 = identifiers2[(0..i)]
        index  = build_traverse_index(files1 + files2.reverse, options)
        return index if not index.nil?
      end
    end

    return nil
  end

  def attach(other, fields = nil, options = {})
    options      = Misc.add_defaults options, :in_namespace => false
    in_namespace = options[:in_namespace]

    fields = other.fields - [key_field].concat(self.fields) if fields == :all
    if in_namespace
      fields = other.fields_in_namespace - [key_field].concat(self.fields) if fields.nil?
    else
      fields = other.fields - [key_field].concat(self.fields) if fields.nil?
    end

    Log.high("Attaching fields:#{fields.inspect} from #{other.filename.inspect}.")

    other = other.tsv(:persistence => options[:persist_input] == true) unless TSV === other 
    case
    when key_field == other.key_field
      attach_same_key other, fields
    when (not in_namespace and self.fields.include?(other.key_field))
      Log.medium "Found other's key field: #{other.key_field}"
      attach_source_key other, other.key_field, fields
    when (in_namespace and self.fields_in_namespace.include?(other.key_field))
      Log.medium "Found other's key field in #{in_namespace}: #{other.key_field}"
      attach_source_key other, other.key_field, fields
    else
      index = TSV.find_traversal(self, other, options)
      raise "Cannot traverse identifiers" if index.nil?
      attach_index other, index, fields
    end
    Log.medium("Attachment of fields:#{fields.inspect} from #{other.filename.inspect} finished.")

    self
  end

  def detach(file)
    file_fields = file.fields.collect{|field| field.fullname}
    detached_fields = []
    self.fields.each_with_index{|field,i| detached_fields << i if file_fields.include? field.fullname}
    reorder :key, detached_fields
  end

  def paste(other, options = {})
    TmpFile.with_file do |output|
      TSV.paste_merge(self, other, output, options[:sep] || "\t")
      tsv = TSV.new output, options
      tsv.key_field = self.key_field unless self.key_field.nil?
      tsv.fields = self.fields + other.fields unless self.fields.nil? or other.fields.nil?
      tsv
    end
  end

  def self.fast_paste(files, delim = "$")
    CMD.cmd("paste #{ files.collect{|f| "'#{f}'"} * " "} -d'#{delim}' |sed 's/#{delim}[^\\t]*//g'", :pipe => true)
  end
end
