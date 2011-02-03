class TSV

  #{{{ Attach Methods
  
  def attach_same_key(other, fields = nil)
    fields = other.fields if fields.nil?

    through do |key, values|
      next unless other.include? key
      new_values = other[key].values_at *fields
      new_values.collect!{|v| [v]}     if     type == :double and not other.type == :double
      new_values.collect!{|v| v.first} if not type == :double and     other.type == :double
      self[key] = self[key].concat new_values
    end

    self.fields = self.fields.concat other.fields.values_at *fields
  end

  def attach_source_key(other, source, fields = nil)
    fields = other.fields if fields.nil?

    through do |key, values|
      source_keys = values[source]
      next if source_keys.nil? or source_keys.empty?

      all_new_values = []
      source_keys.each do |source_key|
        next unless other.include? source_key
        new_values = other[source_key].values_at *fields
        new_values.collect!{|v| [v]}     if     type == :double and not other.type == :double
        new_values.collect!{|v| v.first} if not type == :double and     other.type == :double
        all_new_values << new_values
      end

      next if all_new_values.empty?

      if type == :double
        self[key] = self[key].concat TSV.zip_fields(all_new_values).collect{|l| l.flatten}
      else
        self[key] = self[key].concat all_new_values.first
      end
    end

    self.fields = self.fields.concat other.fields.values_at *fields
  end

  def attach_index(other, index, fields = nil)
    fields = other.fields if fields.nil?

    other = other.tsv unless TSV === other
    field_positions = fields.collect{|field| other.identify_field field}
    field_names     = field_positions.collect{|pos| pos == :key ? other.key_field : other.fields[pos] }

    through do |key, values|
      source_keys = index[key]
      next if source_keys.nil? or source_keys.empty?

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
        new_values.collect!{|v| [v]}     if     type == :double and not other.type == :double
        new_values.collect!{|v| v.first} if not type == :double and     other.type == :double
        all_new_values << new_values
      end

      next if all_new_values.empty?

      if type == :double
        self[key] = self[key].concat TSV.zip_fields(all_new_values).collect{|l| l.flatten}
      else
        self[key] = self[key].concat all_new_values.first
      end
    end

    self.fields = self.fields.concat field_names
  end

  #{{{ Attach Helper
 
  # May make an extra index!
  def self.find_path(files)
    ids = files.collect{|f| f.all_fields}
    id_list = []

    ids.each_with_index do |list, i|
      break if i == ids.length - 1
      match = list & ids[i + 1]
      return nil if match.empty?
      id_list << match.first
    end
    
    if id_list.last.first != files.last.all_fields.first
      id_list << files.last.all_fields.first
      id_list.zip(files)
    else
      id_list.zip(files[0..-1])
    end
  end

  def self.build_traverse_index(files)
    path = find_path(files)

    return nil if path.nil?
    
    traversal_ids = path.collect{|p| p.first}
    
    Log.medium "Found Traversal: #{traversal_ids * " => "}"

    current_key = files.first.all_fields.first
    target = files.last.all_fields.first
    target = nil
    current_id, current_file = path.shift
    index   = current_file.index :target => current_id, :fields =>  current_key, :persistence => false

    while not path.empty?
      current_id, current_file = path.shift
      current_index   = current_file.index :target => current_id, :fields => index.fields.first, :persistence =>  true
      index.process 0 do |value|
        current_index.values_at(*value).flatten.uniq
      end
      index.fields = current_index.fields
    end


    index
  end

  def self.find_traversal(tsv1, tsv2)
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
        index  = build_traverse_index(files1 + files2.reverse)
        return index if not index.nil?
      end
    end

    return nil
  end

  def attach(other, fields = nil)
    case
    when key_field == other.key_field
      attach_same_key other, fields
    when self.fields.include?(other.key_field)
      attach_source_key other, other.key_field, fields
    else
      index = TSV.find_traversal(self, other)
      raise "Cannot traverse identifiers" if index.nil?
      attach_index other, index, fields
    end
  end

end
