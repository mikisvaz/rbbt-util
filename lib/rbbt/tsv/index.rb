require 'rbbt/fix_width_table'
require 'rbbt/util/misc'

require 'rbbt/persist'
require 'rbbt/persist/tsv'

require 'rbbt/tsv/manipulate'
require 'rbbt/tsv/filter'

module TSV

  def index(options = {})
    options = Misc.add_defaults options, 
      :target => :key, :fields => nil, :type => :single, :order => false

    persist_options = Misc.pull_keys options, :persist
    persist_options[:prefix] ||= "Index[#{options[:target] || :key}]"

    Log.debug "Index: #{ filename } - #{options.inspect}"
    Persist.persist_tsv self, filename, options, persist_options do |new|
      with_unnamed do
        target, fields, index_type, order = Misc.process_options options, :target, :fields, :type, :order

        new.serializer = index_type if new.respond_to? :serializer and new.serializer == :type

        if order

          # Maybe best to do the stuff in memory first instead of the original
          # object, which could be persisted
          save = new
          new = {} 

          new_key_field, new_fields = through target, fields, true do |key, values|
            next if key.empty?
            if type == :single
              values = [values]
              values.unshift key
            else
              values = values.dup
              values.unshift [key]
            end

            values.each_with_index do |list, i|
              list = [list] unless type == :double

              list.uniq.each do |value|
                if new.include? value
                  new_value = new[value]
                else
                  new_value = []
                end

                if new_value[i].nil?
                  new_value[i] =  key
                else
                  new_value[i] += "|" <<  key 
                end
                new[value] = new_value
              end
            end
          end

          # Update original object
          new.each do |key, values|
            case
            when index_type == :double
              save[key] = [values.compact.collect{|v| v.split "|"}.flatten.uniq]
            when index_type == :flat
              save[key] = values.compact.collect{|v| v.split "|"}.flatten.uniq
            when index_type == :single
              save[key] = values.compact.collect{|v| v.split "|"}.flatten.first
            end
          end

          new = save
        else
          new_key_field, new_fields = through target, fields, true do |key, values|
            case
            when type == :single
              values = [values]
            when type == :double
              values = values.flatten
            else
              values = values.dup
            end

            values.unshift key

            values.uniq.each do |value|
              case index_type
              when :double
                if not new.include? value
                  new[value] = [[key]]
                else
                  current = new[value]
                  current[0] << key
                  new[value] = current
                end
              when :flat
                if not new.include? value
                  new[value] = [key]
                else
                  current = new[value]
                  current << key
                  new[value] = current
                end

              else
                new[value] = key unless new.include? value
              end
            end
          end
        end

        TSV.setup(new, :type => index_type, :filename => filename, :fields => [new_key_field], :key_field => new_fields * ", ")
      end
    end
  end

  def self.index(file, options = {})
    persist_options = Misc.pull_keys options, :persist
    persist_options[:prefix] ||= "StaticIndex[#{options[:target] || :key}]"
     
    Log.debug "Static Index: #{ file } - #{options.inspect}"
    Persist.persist_tsv nil, file, options, persist_options do |data|
      data_options = Misc.pull_keys options, :data
      identifiers = TSV.open(file, data_options)
      identifiers.with_monitor :desc => "Creating Index for #{ file }" do
        identifiers.index(options.merge :persist_data => data, :persist => persist_options[:persist])
      end
    end
  end

  def pos_index(pos_field = nil, options = {})
    pos_field ||= "Position"

    options = Misc.add_defaults options,
      :persist => false, :persist_file => nil, :persist_update => false 

    persist_options = Misc.pull_keys options, :persist
    persist_options[:prefix] ||= "PosIndex[#{pos_field}]"

    Persist.persist(filename || self.object_id.to_s, :fwt, persist_options) do 
      max_key_size = 0
      index_data = []
      with_unnamed do
        with_monitor :desc => "Creating Index Data", :step => 10000 do
          through :key, pos_field do |key, values|
            key_size = key.length
            max_key_size = key_size if key_size > max_key_size

            pos = values.first
            if Array === pos
              pos.each do |p|
                index_data << [key, p.to_i]
              end
            else
              index_data << [key, pos.to_i]
            end
          end
        end
      end

      index = FixWidthTable.get(:memory, max_key_size, false)
      index.add_point index_data
      index.read
      index
    end
  end

  def self.pos_index(file, pos_field = nil, options = {})
    pos_field ||= "Position"

    data_options = Misc.pull_keys options, :data
    filename = case
               when (String === file or Path === file)
                 file
               when file.respond_to?(:filename)
                 file.filename
               else
                 file.object_id.to_s
               end
    persist_options = Misc.pull_keys options, :persist
    persist_options[:prefix] ||= "StaticPosIndex[#{pos_field}]"

    filters = Misc.process_options options, :filters

    if filters
      filename += ":Filtered[#{filters.collect{|f| f * "="} * ", "}]"
    end

    Persist.persist(filename, :fwt, persist_options) do
      tsv = TSV.open(file, data_options)
      if filters
        tsv.filter
        filters.each do |match, value|
          tsv.add_filter match, value
        end
      end
      tsv.pos_index(pos_field, options)
    end
  end

  def range_index(start_field = nil, end_field = nil, options = {})
    start_field ||= "Start"
    end_field ||= "End"

    options = Misc.add_defaults options,
      :persist => false, :persist_file => nil, :persist_update => false 

    persist_options = Misc.pull_keys options, :persist
    persist_options[:prefix] ||= "RangeIndex[#{start_field}-#{end_field}]"

    Persist.persist(filename || self.object_id.to_s, :fwt, persist_options) do 
      max_key_size = 0
      index_data = []
      with_unnamed do
        with_monitor :desc => "Creating Index Data", :step => 10000 do
          through :key, [start_field, end_field] do |key, values|
            key_size = key.length
            max_key_size = key_size if key_size > max_key_size

            start_pos, end_pos = values
            if Array === start_pos
              start_pos.zip(end_pos).each do |s,e|
                index_data << [key, [s.to_i, e.to_i]]
              end
            else
              index_data << [key, [start_pos.to_i, end_pos.to_i]]
            end
          end
        end
      end

      index = FixWidthTable.get(:memory, max_key_size, true)
      index.add_range index_data
      index.read
      index
    end
  end

  def self.range_index(file, start_field = nil, end_field = nil, options = {})
    start_field ||= "Start"
    end_field ||= "End"

    data_options = Misc.pull_keys options, :data
    filename = case
               when (String === file or Path === file)
                 file
               when file.respond_to?(:filename)
                 file.filename
               else
                 file.object_id.to_s
               end
    persist_options = Misc.pull_keys options, :persist
    persist_options[:prefix] ||= "StaticRangeIndex[#{start_field}-#{end_field}]"

    filters = Misc.process_options options, :filters

    if filters
      filename += ":Filtered[#{filters.collect{|f| f * "="} * ", "}]"
    end

    Persist.persist(filename, :fwt, persist_options) do
      tsv = TSV.open(file, data_options)
      if filters
        tsv.filter
        filters.each do |match, value|
          tsv.add_filter match, value
        end
      end
 
      tsv.range_index(start_field, end_field, options)
    end
  end


#  def self.field_matches(tsv, values)
#    values = [values] if not Array === values
#    Log.debug "Matcing #{values.length} values to #{tsv.filename}"
#
#    if values.flatten.sort[0..9].compact.collect{|n| n.to_i} == (1..10).to_a
#      return {}
#    end
#
#    key_field = tsv.key_field
#    fields = tsv.fields
#
#    field_values = {}
#    fields.each{|field|
#      field_values[field] = []
#    }
#
#    if tsv.type == :double
#      tsv.through do |key,entry_values|
#        fields.zip(entry_values).each do |field,entry_field_values|
#          field_values[field].concat entry_field_values unless entry_field_values.nil?
#        end
#      end
#    else
#      tsv.through do |key,entry_values|
#        fields.zip(entry_values).each do |field,entry_field_values|
#          field_values[field] << entry_field_values
#        end 
#      end
#    end
#
#    field_values.each do |field,field_value_list|
#      field_value_list.replace(values & field_value_list.flatten.uniq)
#    end
#
#    field_values[key_field] = values & tsv.keys
#
#    field_values
#  end
#
#  def field_matches(values)
#    TSV.field_matches(self, values)
#  end
#
#  def guess_field(values)
#    field_matches(values).sort_by{|field, matches| matches.uniq.length}.last
#  end
#
#  def pos_index(pos_field = nil, options = {})
#    pos_field ||= "Position"
#
#    options = Misc.add_defaults options,
#      :persistence => true, :persistence_file => nil, :persistence_update => false 
#
#    prefix = "Pos[#{pos_field}]"
#
#    Persistence.persist(filename, prefix, :fwt, options.merge({
#      :pos_field => pos_field,
#      :filters => (self.respond_to?(:filters)? filters.collect{|f| [f.match, f.value]} : [])
#    })) do |file, options, filename|
#      pos_field = options[:pos_field]
#      value_size = 0
#      index_data = []
#
#      through :key, pos_field do |key, values|
#        value_size = key.length if key.length > value_size
#
#        pos = values.first
#        if Array === pos
#          pos.each do |p|
#            index_data << [key, p.to_i]
#          end
#        else
#          index_data << [key, pos.to_i]
#        end
#      end
#
#      index = FixWidthTable.get(:memory, value_size, false)
#      index.add_point index_data
#      index.read
#      index
#    end
#  end
#
#  def self.pos_index(file, pos_field = nil, options = {})
#    options = Misc.add_defaults options,
#      :persistence => true, :persistence_file => nil, :persistence_update => false, :persistence_source => file, :tsv_serializer => :list,
#      :data_persistence => false, :data_persistence_file => nil, :data_persistence_update => false, :data_persistence_source => file
#
#    #options_data = {
#    #  :cast               => Misc.process_options(options, :data_cast),
#    #  :persistence        => Misc.process_options(options, :data_persistence),
#    #  :monitor            => Misc.process_options(options, :data_monitor),
#    #  :persistence_file   => Misc.process_options(options, :data_persistence_file),
#    #  :persistence_update => Misc.process_options(options, :data_persistence_update),
#    #  :in_situ_persistence => Misc.process_options(options,:data_in_situ_persistence),
#    #  :persistence_source => Misc.process_options(options, :data_persistence_source),
#    #  :importtsv => Misc.process_options(options, :data_importtsv),
#    #}
#
#    options_data = Misc.pull_data_keys(options)
#
#    prefix = "Pos[#{pos_field}]"
#
#    new = Persistence.persist(file, prefix, :fwt, options.merge({:pos_field => pos_field})) do |file, options, filename|
#      tsv = TSV.new(file, :list, options_data)
#
#      if options.include?(:filters) and Array === options[:filters] and not options[:filters].empty?
#        tsv.filter
#        options[:filters].each do |match, value, persistence|
#          tsv.add_filter(match, value, persistence)
#        end
#      end
#
#      tsv.pos_index options[:pos_field], options.merge(:persistence => false, :persistence_file => nil)
#    end
#  end
#
#  def range_index(start_field = nil, end_field = nil, options = {})
#    start_field ||= "Start"
#    end_field ||= "End"
#    options = Misc.add_defaults options,
#      :persistence => true, :persistence_file => nil, :persistence_update => false 
#
#    prefix = "Range[#{start_field}-#{end_field}]"
#
#    Persistence.persist(filename, prefix, :fwt, options.merge({
#      :start_field => start_field, :end_field => end_field,
#      :filters => (self.respond_to?(:filters)? filters.collect{|f| [f.match, f.value]} : [])
#    })) do |file, options, filename|
#      start_field, end_field = options.values_at :start_field, :end_field
#
#      value_size = 0
#      index_data = []
#
#      through :key, [start_field, end_field] do |key, values|
#        value_size = key.length if key.length > value_size
#
#        start_pos, end_pos = values
#
#        if Array === start_pos
#          start_pos.zip(end_pos).each do |s,e|
#            index_data << [key, [s.to_i, e.to_i]]
#          end
#        else
#          index_data << [key, [start_pos.to_i, end_pos.to_i]]
#        end
#      end
#
#      index = FixWidthTable.get(:memory, value_size, true)
#      index.add_range index_data
#      index.read
#      index
#    end
#  end
#
#  def self.range_index(file, start_field = nil, end_field = nil, options = {})
#    options = Misc.add_defaults options,
#      :persistence => true, :persistence_file => nil, :persistence_update => false, :persistence_source => file, :tsv_serializer => :list,
#      :data_persistence => false, :data_persistence_file => nil, :data_persistence_update => false, :data_persistence_source => file
#
#    options_data = {
#      :persistence        => Misc.process_options(options, :data_persistence),
#      :persistence_file   => Misc.process_options(options, :data_persistence_file),
#      :persistence_update => Misc.process_options(options, :data_persistence_update),
#      :persistence_source => Misc.process_options(options, :data_persistence_source),
#    }
#
#    prefix = "Range[#{start_field}-#{end_field}]"
#
#    options_data[:type] = :flat if options[:order] == false
#
#    Persistence.persist(file, prefix, :fwt, options.merge({:start_field => start_field, :end_field => end_field})) do |file, options, filename|
#      tsv = TSV.new(file, :list, options_data)
#
#      if options.include?(:filters) and Array === options[:filters] and not options[:filters].empty?
#        tsv.filter
#        options[:filters].each do |match, value, persistence|
#          tsv.add_filter(match, value, persistence)
#        end
#      end
#
#      tsv.range_index options[:start_field], options[:end_field], options.merge(:persistence => false, :persistence_file => nil)
#    end
#  end
#
end

