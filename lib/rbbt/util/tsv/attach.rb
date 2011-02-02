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

  def self.create_index2(tsv1, tsv2)
    path = find_indentifier_path(tsv1, tsv2)

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
