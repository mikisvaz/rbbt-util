require 'rbbt/util/tsv/manipulate'

class TSV

  def index(options = {})
    options = Misc.add_defaults options, :order => false, :persistence => false, :target => :key, :fields => nil, :case_insensitive => true, :tsv_serializer => :list

    new, extra = Persistence.persist(self, :Index, :tsv, options) do |tsv, options, filename|
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
            list.each do |elem|
              elem.downcase if case_insensitive
              new[elem] ||= []
              new[elem][i + 1] ||= []
              new[elem][i + 1].concat keys
            end
          end

          keys.each do |key|
            key = key.downcase if case_insensitive
            new[key]    ||= []
            new[key][0] ||= []
            new[key][0].concat keys
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
          values.unshift type == :double ? [key] : key
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

      [new, {:key_field => new_key_field, :fields => new_fields, :type => :list, :filename => (filename.nil? ? nil : "Index:" + filename), :case_insensitive => case_insensitive}]
    end

    new  = TSV.new(new) if Hash === new
    new.filename = "Index: " + filename.to_s + options.inspect
    new.fields = extra[:fields]
    new.key_field = extra[:key_field]
    new.case_insensitive = extra[:case_insensitive]
    new.type = extra[:type]
    new
  end

  def self.index(file, options = {})
    options = Misc.add_defaults options,
      :persistence => false, :persistence_file => nil, :persistence_update => false, :persistence_source => file, :tsv_serializer => :list,
      :data_persistence => false, :data_persistence_file => nil, :data_persistence_update => false, :data_persistence_source => file

    options_data = {
      :persistence        => Misc.process_options(options, :data_persistence),
      :persistence_file   => Misc.process_options(options, :data_persistence_file),
      :persistence_update => Misc.process_options(options, :data_persistence_update),
      :persistence_source => Misc.process_options(options, :data_persistence_source),
    }

    options_data[:type] = :flat if options[:order] == false

    new = Persistence.persist(file, :Index, :tsv, options) do |file, options, filename|

      index = TSV.new(file, :double, options_data).index options
      index
    end

    new
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

end
