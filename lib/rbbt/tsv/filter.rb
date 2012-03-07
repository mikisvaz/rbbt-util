require 'rbbt/util/misc'
require 'set'
module Filtered

  class FilterArray
    attr_accessor :filters

    def ids
      ids = filters.inject(nil){|list,filter| list.nil? ? filter.ids.dup : Misc.merge_sorted_arrays(list, filter.ids.dup)}
    end

    def method_missing(name, *args)
      filters.each do |filter|
        filter.send(name, *args)
      end
    end
  end

  #{{{ FILTER

  class Filter
    attr_accessor :data, :match, :fieldnum, :value, :list, :unsaved
    attr_accessor :persistence

    def initialize(data, match, value, persistence = nil)
      @data = data
      @match = match
      @value = value
      @unsaved = []

      case 
      when Hash === persistence
        @persistence = persistence
      when String === persistence
        @persistence = TSV.setup Persist.open_tokyocabinet(persistence, false, :list)
        @persistence.read
      end

      @list = nil
      case
      when @match == :key
        @value = Set.new(@value)
        class << self
          self
        end.class_eval <<-EOC
          def match_entry(key, entry)
            key == @value or (Set === @value and @value.include? key)
          end
        EOC
      when @match.match(/field:(.*)/)
        @fieldnum = data.identify_field $1
        class << self
          self
        end.class_eval <<-EOC
          def match_entry(key, entry)
            value = entry[@fieldnum] 
            value == @value or (Array === value and value.include? @value)
          end
        EOC
      end
    end

    def key
      case 
      when String === value
        value
      else
        Marshal.dump(value)
      end
    end

    def save(ids)
      if persistence
        persistence.write
        persistence[self.key] = ids
        persistence.read
      else
        if @list.nil?
          @list = ids
        else
          @list.replace ids
        end
      end
    end

    def update
      ids = []

      data.with_unnamed do
        data.unfiltered_each do |key, entry|
          ids << key if match_entry(key, entry)
        end
      end

      save(ids.sort)
    end

    def saved
      if persistence.nil?
        return nil if list.nil?
        list
      else
        return nil if not persistence.include?(self.key)
        persistence[self.key]
      end
    end

    def add_unsaved
      save(Misc.merge_sorted_arrays(unsaved.sort, saved || [])) if unsaved.any?
      unsaved.clear
    end

    def ids
      add_unsaved

      list = saved
      if list.nil?
        update
        list = saved
      end
      list
    end

    def add(id)
      unsaved.push id
    end

    def clean
      add_unsaved
      if persistence and persistence.include? self.key
        restore = ! persistence.write?
        persistence.write unless persistence.write?
        persistence.delete self.key
        persistence.read if restore
      else
        @list = nil
      end
    end

    def reset
      add_unsaved
      if persistence
        persistence.clear
      else
        @list = nil
      end
    end
  end

  #}}} FILTER

  def self.extended(base)
    if not base.respond_to? :unfiltered_set
      class << base
        attr_accessor :filter_dir, :filters
        
        alias unfiltered_set []=
        alias []= filtered_set

        alias unfiltered_filename filename
        alias filename filtered_filename

        alias unfiltered_keys keys
        alias keys filtered_keys

        alias unfiltered_values values
        alias values filtered_values

        alias unfiltered_each each
        alias each filtered_each

        alias unfiltered_collect collect
        alias collect filtered_collect

        alias unfiltered_delete delete
        alias delete filtered_delete
      end
    end
    base.filters = []
  end

  def filtered_filename
    if filters.empty?
      unfiltered_filename
    else
      unfiltered_filename + ":Filtered[#{filters.collect{|f| [f.match, Array === f.value ? Misc.hash2md5(:values => f.value) : f.value] * "="} * ", "}]"
    end
  end

  def filtered_set(key, value)
    if filters.empty?
      self.send(:unfiltered_set, key, value)
    else
      filters.each do |filter| 
        filter.add key if filter.match_entry key, value
      end
      self.send(:unfiltered_set, key, value)
    end
  end

  def filtered_keys
    with_monitor(false) do
      if filters.empty?
        self.send(:unfiltered_keys)
      else
        filters.inject(nil){|list,filter| list.nil? ? filter.ids.dup : Misc.intersect_sorted_arrays(list, filter.ids.dup)}
      end
    end
  end

  def filtered_values 
    if filters.empty?  
      self.send(:unfiltered_values) 
    else 
      ids = filters.inject(nil){|list,filter| list.nil? ? filter.ids.dup : Misc.intersect_sorted_arrays(list, filter.ids.dup)}
      self.send :values_at, *ids
    end
  end

  def filtered_each(&block)
    if filters.empty?
      self.send(:unfiltered_each, &block)
    else
      ids = filters.inject(nil){|list,filter| list.nil? ? filter.ids.dup : Misc.intersect_sorted_arrays(list, filter.ids.dup)}

      ids.each do |id|
        value = self[id]
        yield id, value if block_given?
        [id, value]
      end
    end
  end

  def filtered_collect(&block)
    if filters.empty?
      self.send(:unfiltered_collect, &block)
    else
      ids = filters.inject(nil){|list,filter| list = (list.nil? ? filter.ids.dup : Misc.intersect_sorted_arrays(list, filter.ids.dup))}

      new = TSV.setup({}, self.options)

      ids.zip(self.send(:values_at, *ids)).each do |id, values|
        new[id] = values
      end
      new.send :collect, &block
    end
  end

  def filtered_delete(key)
    if filters.empty?
      self.send(:unfiltered_delete, key)
    else
      reset_filters
      self.send :unfiltered_delete, key
    end
  end

  def add_filter(match, value, persistence = nil)
    if persistence.nil? and filter_dir
      persistence = File.join(filter_dir, match.to_s + '.filter')
    end

    filter = Filter.new self, match, value, persistence
    filters.push filter
  end

  def pop_filter
    filters.pop.add_unsaved if filters.any?
  end

end

module TSV
  def filter(filter_dir = nil)
    self.extend Filtered
    self.filter_dir = filter_dir
    self.filters = []
    self
  end

  def reset_filters
    if filter_dir.nil? or filter_dir.empty?
      filters.each do |filter| filter.reset end
      return
    end

    Dir.glob(File.join(filter_dir, '*.filter')).each do |f|
      FileUtils.rm f
    end
  end
end

