module TSV
  
  def attach_same_key(other, fields = nil)
    fields = other.fields - [key_field].concat(self.fields) if fields.nil?

    fields = [fields].compact unless Array === fields

    field_positions = fields.collect{|field| other.identify_field field}
    other.with_unnamed do
      with_unnamed do
        through do |key, values|
          if other.include? key
            new_values = other[key].values_at *field_positions
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
      end
    end

    self.fields = self.fields.concat other.fields.values_at *fields
  end

  def attach_source_key(other, source, options = {})
    fields = Misc.process_options options, :fields
    one2one = Misc.process_options options, :one2one

    fields = other.fields - [key_field].concat(self.fields) if fields.nil?

    other = other.tsv(:persistence => :no_create) unless TSV === other
    field_positions = fields.collect{|field| other.identify_field field}
    field_names     = field_positions.collect{|pos| pos == :key ? other.key_field : other.fields[pos] }

    source_pos = identify_field source

    other.with_unnamed do
      with_unnamed do
        through do |key, values|
          source_keys = values[source_pos]

          case
          when (source_keys.nil? or (Array === source_keys and source_keys.empty?))
            if type == :double
              self[key] = values.concat field_positions.collect{|v| []}
            else
              self[key] = values.concat [nil] * field_positions
            end
          when Array === source_keys
            all_new_values = source_keys.collect do |source_key|
              positions = field_positions.collect do |pos|
                if pos == :key
                  [source_key]
                else
                  if other.include? source_key
                    v = other[source_key][pos]
                    Array === v ? v : [v]
                  else
                    [nil]
                  end
                end
              end

              positions.collect!{|v| v[0..0]} if one2one
              positions
            end

            new = Misc.zip_fields(all_new_values).each{|field_entry|
              field_entry.flatten!
            }

            self[key] = values.concat new
          else
            source_key = source_keys
            all_new_values = field_positions.collect do |pos|
              if pos == :key
                source_key
              else
                if other.include? source_key
                  v = other[source_key][pos]
                  Array === v ? v.first : v
                else
                  nil
                end
              end
            end

            self[key] = values.concat all_new_values
          end

        end
      end
    end

    self.fields = self.fields.concat field_names
    self
  end

  def attach_index(other, index, fields = nil)
    fields = other.fields - [key_field].concat(self.fields) if fields.nil?
    fields = [fields] unless Array === fields

    other = other.tsv unless TSV === other
    field_positions = fields.collect{|field| other.identify_field field}
    field_names     = field_positions.collect{|pos| pos == :key ? other.key_field : other.fields[pos] }

    length = self.fields.length
    other.with_unnamed do
      index.with_unnamed do
        with_unnamed do
          through do |key, values|
            source_keys = index[key]
            source_keys = [source_keys] unless Array === source_keys
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

            current = self[key] || [[]] * fields.length

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
        end
      end
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
      ids = files.collect{|f| f.all_fields }
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

    Log.low "Found Traversal: #{traversal_ids * " => "}"

    data_key, data_file = path.shift
    data_index = if data_key == data_file.key_field
                   Log.debug "Data index not required '#{data_file.key_field}' => '#{data_key}'"
                   nil
                 else
                   Log.debug "Data index required"
                   data_file.index :target => data_key, :fields => [data_file.key_field], :persist => false
                 end

    current_index = data_index
    current_key   = data_key
    while not path.empty?
      next_key, next_file = path.shift

      if current_index.nil?
        current_index = next_file.index(:target => next_key, :fields => [current_key], :persist => persist_input)
        current_index = current_index.select :key => data_file.keys
      else
        next_index = next_file.index :target => next_key, :fields => [current_key], :persist => persist_input

        next_index.with_unnamed do
          current_index.with_unnamed do
            current_index.process current_index.fields.first do |values|
              if values.nil?
                nil
              else
                next_index.values_at(*values).flatten.collect
              end
            end
            current_index.fields = [next_key]
          end
        end
      end
      current_key = next_key
    end

    current_index
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

end
