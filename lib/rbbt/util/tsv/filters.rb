require 'rbbt/util/misc'
module Filtered

  class Filter
    attr_accessor :data, :match, :fieldnum, :value, :list, :unsaved
    attr_accessor :persistence
    def initialize(data, match, value, persistence = nil)
      @data = data
      @value = value
      @unsaved = []

      case 
      when Hash === persistence
        @persistence = persistence
      when String === persistence
        @persistence = TSV.new TCHash.get(persistence)
      end

      @list = nil
      case
      when match.match(/field:(.*)/)
        field_num = data.identify_field $1
        Misc.add_method(self, :match) do |entry|
          entry[field_num] == value
        end
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
        ids << key if match(entry)
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

    def ids
      list = saved
      if list.nil?
        update
        list = saved
      end

      if unsaved.any?
        save(Misc.merge_sorted_arrays(unsaved.sort, list))
        unsaved.clear
        list = saved
      end
      list
    end

    def add(id)
      unsaved << id
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
          filter.add key if filter.match value
        end
        self.send(:unfiltered_set, key, value)
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
  end

  def add_filter(match, value, persistence = nil)
    filters.push Filter.new self, match, value, persistence
  end

end

class TSV
  def filter(filter_dir = nil)
    self.extend Filtered
    self.filter_dir = filter_dir
    self.filters = []
    self
  end
end

