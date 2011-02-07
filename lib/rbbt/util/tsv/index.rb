require 'rbbt/util/tsv/manipulate'
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

    new = Persistence.persist(self, prefix, :tsv, options) do |tsv, options, filename|
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
          values.flatten!
          values.compact!
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
                                when String === match
                                  [match, other.index]
                                when Array === match
                                  [match.first, other.index(:fields => match.last)]
                                end

    match_source_position = identify_field match_source
                                 
    # through
    new = {}
    each do |key,values|
      source_keys = match_source == :key ? key : values[match_source]
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
            other_key
          else
            other[other_key][field]
          end
        end
      end.compact

      if type == :double
        new_values = values + TSV.zip_fields(other_values)
      else
        new_values = values + TSV.zip_fields(other_values).collect{|v| v.first}
      end
      new[key] = new_values
    end

    new = TSV.new new
    new.fields = fields + new_fields if fields
    new.key_field = key_field if key_field
    new.type = type

    new
  end

  def self.field_matches(tsv, values)
    if values.flatten.sort[0..9].compact.collect{|n| n.to_i} == (1..10).to_a
      return {}
    end

    key_field = tsv.key_field
    fields = tsv.fields

    field_values = {}
    fields.each{|field|
      field_values[field] = []
    }

    if type == :double
      tsv.through do |key,entry_values|
        fields.zip(entry_values).each do |field,entry_field_values|
          field_values[field].concat entry_field_values
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

  def sorted_index(pos_start = nil, pos_end = nil)
    raise "Please specify indexing fields" if (pos_start.nil? and fields.length > 2)

    case
    when (pos_start.nil? and pos_end.nil? and fields.length == 2)
      pos_start = fields.first
      pos_end   = fields.last
    when (pos_start.nil? and pos_end.nil? and fields.length == 1)
      pos_start = fields.first
    end

    range = ! pos_end.nil?

    index = Persistence.persist(filename, "SortedIndex[#{range ? pos_start + ":" + pos_end: pos_start}]", :fwt, :start => pos_start, :end => pos_end, :range => range) do |filename, options|
      pos_start, pos_end, range = Misc.process_options options, :start, :end, :range
      data = case
             when (type == :double and range)
               collect do |key, values|
                 p_start, p_end = values.values_at pos_start, pos_end
                 next if p_start.nil? or p_end.nil? or p_start.empty? or p_end.empty?
                 [[p_start.first, p_end.first], key]
               end
             when (type == :double and not range)
               collect do |key, values|
                 p_start = values.values_at pos_start
                 next if p_start.nil? or p_start.empty? 
                 [p_start.first, key]
               end
             when range
               slice [pos_start, pos_end]
             else
               slice pos_start
             end
      data
    end

    index
  end


end

