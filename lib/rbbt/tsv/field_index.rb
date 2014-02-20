
module TSV
  class << self
    attr_accessor :field_index_dir
    def field_index_dir
      @field_index_dir ||= Rbbt.var.cache.field_indices
    end
  end

  attr_accessor :field_indices


  def field_index(field)
    @field_indices ||= {}
    @field_indices[field] ||= Persist.persist_tsv(self, filename, {:field => field}, :prefix => "FieldIndex", :dir => TSV.field_index_dir, :persist => true, :serializer => :list, :engine => "BDB" ) do |data|
      tsv = {}
      case type 
      when :single, :list
        through :key, [field] do |key, values|
          value = values.first
          tsv[value] ||= []
          tsv[value] << key
        end
      else
        through :key, [field] do |key, values|
          values.first.each do |value|
            tsv[value] ||= []
            tsv[value] << key
          end
        end
      end

      tsv.each do |v,keys|
        data[v] = keys.sort
      end

      data
    end
  end

  def field_index_select(matches)
    final = nil
    matches.each do |field,values|
      i = field_index(field)

      if Array === values
        keys = values.inject([]){|acc,value| m = i[value]; acc = m.nil? ? acc : Misc.merge_sorted_arrays( acc, m) }
      else
        keys = i[values] || []
      end

      final = final.nil? ? keys : Misc.intersect_sorted_arrays(final, keys)
    end
    final
  end
end
