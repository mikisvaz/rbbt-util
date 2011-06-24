require 'rbbt/util/tsv/manipulate'
require 'rbbt/util/tsv/filters'
require 'rbbt/util/fix_width_table'

class TSV

  def index(options = {})
    options = Misc.add_defaults options, :order => false, :persistence => true, :target => :key, :fields => nil, :case_insensitive => case_insensitive, :tsv_serializer => :list

    prefix = case
             when options[:target]
               "Index[#{options[:target]}]"
             else
               "Index[:key]"
             end

    Persistence.persist(self, prefix, :tsv, options) do |tsv, options, filename|
      order, target, fields, case_insensitive = Misc.process_options options, :order, :target, :fields, :case_insensitive

      new = {}
      
      ## Ordered
      if order

        # through 
         
        new_key_field, new_fields = through target, fields do |key, values|
          if Array === key
            keys = key
          else
            keys = [key]
          end

          values.each_with_index do |list,i|
            list = [list] unless Array === list
            i += 1 if fields.nil?
            list.each do |elem|
              next if elem.nil? or elem.empty?
              elem.downcase if case_insensitive
              new[elem] ||= []
              new[elem][i] ||= []
              new[elem][i].concat keys
            end
          end

          if fields.nil?
            keys.each do |key|
              key = key.downcase if case_insensitive
              new[key]    ||= []
              new[key][0] ||= []
              new[key][0].concat keys
            end
          end

        end

        # flatten

        new.each do |key, values| 
          new[key] = values.flatten.compact
        end

      ## Not ordered
      else
        double_keys = true unless type != :double or identify_field(target) == :key
        new.each do |key, fields| fields.flatten! end

        new_key_field, new_fields = through target, fields do |key, values|
          values.unshift type == :double ? [key] : key if fields.nil?
          if type == :flat
            list = values
          else
            list = values.flatten unless type == :flat
          end
          list.collect!{|e| e.downcase} if case_insensitive
          list.each do |elem|
            next if elem.nil? or elem.empty?
            new[elem] ||= []
            if double_keys
              new[elem].concat key 
            else
              new[elem] << key 
            end
          end
        end

      end

      new.each do |key, values| 
        values.uniq!
      end

      key_field = case
                      when new_key_field
                        new_key_field + "|" + new_fields * "|"
                      else
                        nil
                      end

      fields = case
                   when new_key_field.nil?
                     nil
                   else
                     [new_key_field]
                   end

      new = TSV.new([new, {:namespace => namespace, :key_field => key_field, :fields => fields, :type => :flat, :filename => (filename.nil? ? nil : "Index:" + filename), :case_insensitive => case_insensitive}])

      new
    end
  end

  def self.index(file, options = {})
    options = Misc.add_defaults options,
      :persistence => true, :persistence_file => nil, :persistence_update => false, :persistence_source => file, :tsv_serializer => :list,
      :data_persistence => false, :data_persistence_file => nil, :data_persistence_update => false, :data_persistence_source => file

    options_data = {
      :persistence        => Misc.process_options(options, :data_persistence),
      :persistence_file   => Misc.process_options(options, :data_persistence_file),
      :persistence_update => Misc.process_options(options, :data_persistence_update),
      :persistence_source => Misc.process_options(options, :data_persistence_source),
    }

    options_data[:type] = :flat if options[:order] == false

    prefix = case
             when options[:target]
               "Index_static[#{options[:target]}]"
             else
               "Index_static[:key]"
             end

    new = Persistence.persist(file, prefix, :tsv, options) do |file, options, filename|
      TSV.new(file, :double, options_data).index options.merge :persistence => false, :persistence_file => nil
    end
  end


  def smart_merge(other, match = nil, fields2add = nil)

    # Determine new fields
    #    both have fields => list of names
    #    not both have fields => nil

    #    fields2add = case
    #                 when (fields2add.nil? and (other.fields.nil? or self.fields.nil?))
    #                   nil
    #                 when fields2add.nil?
    #                   other.all_fields
    #                 else
    #                   fields2add
    #                 end

    # Determine common fields
    
    common_fields, new_fields = case
                                when fields2add != nil
                                  [fields & other.fields, fields2add]
                                when (other.all_fields.nil? or self.all_fields.nil?)
                                  [nil, other_fields]
                                else
                                  [(all_fields & other.all_fields), (other.all_fields - all_fields)]
                                end
    
    # Load matching scheme. Index and source field

    match_source, match_index = case
                                when (match.nil? and not key_field.nil? and other.key_field == key_field)
                                  [:key, nil]
                                when match.nil?
                                  [:key, other.index]
                                when TSV === match
                                  raise "No field info in match TSV" if match.fields.nil?
                                  match_source = (all_fields & match.all_fields).first
                                  index = match.index :target => other.key_field, :fields => match_source
                                  [match_source, index]
                                when (String === match and match == key_field)
                                  [:key, other.index]
                                when String === match
                                  [match, other.index]
                                when Array === match
                                  [match.first, other.index(:fields => match.last)]
                                end

    match_source_position = identify_field match_source
                                 
    # through
    new = {}
    each do |key,values|
      source_keys = match_source == :key ? key : values[match_source_position]
      source_keys = [source_keys] unless Array === source_keys
      other_keys = case
                   when index.nil?
                     source_keys
                   else
                     index.values_at(*source_keys).flatten.compact
                   end

      other_keys = other_keys.collect do |other_key| match_index[other_key] end.flatten unless match_index.nil?


      other_values = other_keys.collect do |other_key|
        next unless other.include? other_key
        new_fields.collect do |field|
          if field == other.key_field
            if type == :double
              [other_key]
            else
              other_key
            end
          else
            other[other_key][field]
          end
        end
      end.compact

      other_values = case
                     when type == :double
                       TSV.zip_fields(other_values).collect{|v| v.flatten.uniq}
                     else
                       TSV.zip_fields(other_values).collect{|v| v.flatten.first}
                     end
      
      new_values = values + other_values

      new[key] = new_values
    end

    new = TSV.new new
    new.fields = fields + new_fields if fields
    new.key_field = key_field if key_field
    new.type = type

    new
  end

  def self.field_matches(tsv, values)
    values = [values] if not Array === values
    Log.debug "Matcing #{values.length} values to #{tsv.filename}"

    if values.flatten.sort[0..9].compact.collect{|n| n.to_i} == (1..10).to_a
      return {}
    end

    key_field = tsv.key_field
    fields = tsv.fields

    field_values = {}
    fields.each{|field|
      field_values[field] = []
    }

    if tsv.type == :double
      tsv.through do |key,entry_values|
        fields.zip(entry_values).each do |field,entry_field_values|
          field_values[field].concat entry_field_values unless entry_field_values.nil?
        end
      end
    else
      tsv.through do |key,entry_values|
        fields.zip(entry_values).each do |field,entry_field_values|
          field_values[field] << entry_field_values
        end 
      end
    end

    field_values.each do |field,field_value_list|
      field_value_list.replace(values & field_value_list.flatten.uniq)
    end

    field_values[key_field] = values & tsv.keys

    field_values
  end

  def field_matches(values)
    TSV.field_matches(self, values)
  end

  def guess_field(values)
    field_matches(values).sort_by{|field, matches| matches.uniq.length}.last
  end

  def pos_index(pos_field = nil, options = {})
    pos_field ||= "Position"

    options = Misc.add_defaults options,
      :persistence => true, :persistence_file => nil, :persistence_update => false 

    prefix = "Pos[#{pos_field}]"

    Persistence.persist(filename, prefix, :fwt, options.merge({
      :pos_field => pos_field,
      :filters => (self.respond_to?(:filters)? filters.collect{|f| [f.match, f.value]} : [])
    })) do |file, options, filename|
      pos_field = options[:pos_field]
      value_size = 0
      index_data = []

      through :key, pos_field do |key, values|
        value_size = key.length if key.length > value_size

        pos = values.first
        if Array === pos
          pos.each do |p|
            index_data << [key, p.to_i]
          end
        else
          index_data << [key, pos.to_i]
        end
      end

      index = FixWidthTable.get(:memory, value_size, false)
      index.add_point index_data
      index.read
      index
    end
  end

  def self.pos_index(file, pos_field = nil, options = {})
    options = Misc.add_defaults options,
      :persistence => true, :persistence_file => nil, :persistence_update => false, :persistence_source => file, :tsv_serializer => :list,
      :data_persistence => false, :data_persistence_file => nil, :data_persistence_update => false, :data_persistence_source => file

    options_data = {
      :persistence        => Misc.process_options(options, :data_persistence),
      :persistence_file   => Misc.process_options(options, :data_persistence_file),
      :persistence_update => Misc.process_options(options, :data_persistence_update),
      :persistence_source => Misc.process_options(options, :data_persistence_source),
    }


    prefix = "Pos[#{pos_field}]"

    new = Persistence.persist(file, prefix, :fwt, options.merge({:pos_field => pos_field})) do |file, options, filename|
      tsv = TSV.new(file, :list, options_data)

      if options.include?(:filters) and Array === options[:filters] and not options[:filters].empty?
        tsv.filter
        options[:filters].each do |match, value, persistence|
          tsv.add_filter(match, value, persistence)
        end
      end

      tsv.pos_index options[:pos_field], options.merge(:persistence => false, :persistence_file => nil)
    end
  end

  def range_index(start_field = nil, end_field = nil, options = {})
    start_field ||= "Start"
    end_field ||= "End"
    options = Misc.add_defaults options,
      :persistence => true, :persistence_file => nil, :persistence_update => false 

    prefix = "Range[#{start_field}-#{end_field}]"

    Persistence.persist(filename, prefix, :fwt, options.merge({
      :start_field => start_field, :end_field => end_field,
      :filters => (self.respond_to?(:filters)? filters.collect{|f| [f.match, f.value]} : [])
    })) do |file, options, filename|
      start_field, end_field = options.values_at :start_field, :end_field

      value_size = 0
      index_data = []

      through :key, [start_field, end_field] do |key, values|
        value_size = key.length if key.length > value_size

        start_pos, end_pos = values

        if Array === start_pos
          start_pos.zip(end_pos).each do |s,e|
            index_data << [key, [s.to_i, e.to_i]]
          end
        else
          index_data << [key, [start_pos.to_i, end_pos.to_i]]
        end
      end

      index = FixWidthTable.get(:memory, value_size, true)
      index.add_range index_data
      index.read
      index
    end
  end

  def self.range_index(file, start_field = nil, end_field = nil, options = {})
    options = Misc.add_defaults options,
      :persistence => true, :persistence_file => nil, :persistence_update => false, :persistence_source => file, :tsv_serializer => :list,
      :data_persistence => false, :data_persistence_file => nil, :data_persistence_update => false, :data_persistence_source => file

    options_data = {
      :persistence        => Misc.process_options(options, :data_persistence),
      :persistence_file   => Misc.process_options(options, :data_persistence_file),
      :persistence_update => Misc.process_options(options, :data_persistence_update),
      :persistence_source => Misc.process_options(options, :data_persistence_source),
    }

    prefix = "Range[#{start_field}-#{end_field}]"

    options_data[:type] = :flat if options[:order] == false

    Persistence.persist(file, prefix, :fwt, options.merge({:start_field => start_field, :end_field => end_field})) do |file, options, filename|
      tsv = TSV.new(file, :list, options_data)

      if options.include?(:filters) and Array === options[:filters] and not options[:filters].empty?
        tsv.filter
        options[:filters].each do |match, value, persistence|
          tsv.add_filter(match, value, persistence)
        end
      end

      tsv.range_index options[:start_field], options[:end_field], options.merge(:persistence => false, :persistence_file => nil)
    end
  end

end

