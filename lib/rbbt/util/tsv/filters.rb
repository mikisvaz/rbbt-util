require 'rbbt/util/misc'
module Filtered

  class FilterArray
    attr_accessor :filters

    def ids
      ids = filters.inject(nil){|list,filter| list.nil? ? filter.ids : Misc.merge_sorted_arrays(list, filter.ids.dup)}
    end

    def method_missing(name, *args)
      filters.each do |filter|
        filter.send(name, *args)
      end
    end
  end

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
        @persistence = TSV.new TCHash.get(persistence)
        @persistence.read
      end

      @list = nil
      case
      when @match.match(/field:(.*)/)
        @fieldnum = data.identify_field $1
        class << self
          self
        end.class_eval <<-EOC
          def match_entry(entry)
            entry[@fieldnum] == @value
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
        if list.nil?
          @list = ids
        else
          @list.replace ids
        end
      end
    end

    def update
      ids = []
      data.unfiltered_each do |key, entry|
        ids << key if match_entry(entry)
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

  def self.extended(base)
    class << base
      attr_accessor :filter_dir, :filters
    end

    Misc.redefine_method base, :[]=, :unfiltered_set do |key,value|
      if filters.empty?
        self.send(:unfiltered_set, key, value)
      else
        filters.each do |filter| 
          filter.add key if filter.match_entry value
        end
        self.send(:unfiltered_set, key, value)
      end
    end

    Misc.redefine_method base, :keys, :unfiltered_keys do 
    if filters.empty?
      self.send(:unfiltered_keys)
    else
      filters.inject(nil){|list,filter| list.nil? ? filter.ids : Misc.intersect_sorted_arrays(list, filter.ids.dup)}
    end
    end

    Misc.redefine_method base, :values, :unfiltered_values do
      if filters.empty?
        self.send(:unfiltered_values)
      else
        ids = filters.inject(nil){|list,filter| list.nil? ? filter.ids : Misc.intersect_sorted_arrays(list, filter.ids.dup)}
        self.send :values_at, *ids
    end
    end

    Misc.redefine_method base, :each, :unfiltered_each do |&block|
    if filters.empty?
      self.send(:unfiltered_each, &block)
    else
      ids = filters.inject(nil){|list,filter| list.nil? ? filter.ids : Misc.intersect_sorted_arrays(list, filter.ids.dup)}
      new = self.dup
      new.data = {}

      ids.zip(self.send(:values_at, *ids)).each do |id, values|
        new[id] = values
      end

      new.send :each, &block
    end
    end

    Misc.redefine_method base, :collect, :unfiltered_collect do |&block|
    if filters.empty?
      self.send(:unfiltered_collect, &block)
    else
      ids = filters.inject(nil){|list,filter| list = (list.nil? ? filter.ids : Misc.intersect_sorted_arrays(list, filter.ids))}
      new = self.dup
      new.data = {}
      ids.zip(self.send(:values_at, *ids)).each do |id, values|
        new[id] = values
      end
      new.send :collect, &block
    end
    end

    Misc.redefine_method base, :delete, :unfiltered_delete do |key|
    if filters.empty?
      self.send(:unfiltered_delete, key)
    else
      reset_filters
      self.send :unfiltered_delete, key
    end
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
    filters.pop
  end

end

class TSV
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

