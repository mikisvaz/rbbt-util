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

    through do |key, values|
      source_keys = index[key]
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

  #{{{ Attach Helper
 
  def self.find_path(files)
    ids = files.collect{|f| TSV === f ? f.all_fields : f.tsv_all_fields }
    id_list = []

    ids.each_with_index do |list, i|
      break if i == ids.length - 1
      match = list & ids[i + 1]
      return nil if match.empty?
      id_list << match.first
    end

    id_list.zip(files[0..-1])
  end

  def self.build_traverse_index(files, target = nil)
    path = find_path(files)

    current_id, current_file = path.shift
    index   = current_file.index :target => current_id  

    while not path.empty?
      ddd index
      current_id, current_file = path.shift
      current_index   = current_file.index :target => current_id, :fields => (path.empty? ? target : path.first.first)
      index.process 0 do |value|
        current_index.values_at(*value).flatten.uniq
      end
      ddd index
    end

    index
  end

 
  def self.create_index(tsv1, tsv2)
    identifiers1 = tsv1.identifier_files.first

    identifiers2 = tsv2.identifier_files.first

    index = nil

    case
    when (identifiers2 and identifiers2.tsv_all_fields.include?(tsv1.key_field) and identifiers2.tsv_all_fields.include?(tsv2.key_field))
      index = identifiers2.index :target => tsv2.key_field, :fields => tsv1.key_field
    when (identifiers1 and identifiers1.all_fields.include?(tsv2.key_field) and  identifiers1.all_fields.include?(tsv1.key_field))
      index = identifiers1.index :target => tsv2.key_field, :fields => tsv1.key_field
    else
      raise "Cannot traverse identifiers"
    end


    index
  end

  def attach(other, fields = nil)
    case
    when key_field == other.key_field
      attach_same_key other, fields
    when self.fields.include?(other.key_field)
      attach_source_key other, other.key_field, fields
    else
      index = TSV.create_index self, other
      attach_index other, index, fields
    end
  end

end
