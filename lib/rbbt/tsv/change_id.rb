require 'rbbt/tsv'

module TSV
  def self.change_key(tsv, format, options = {})
    options = Misc.add_defaults options, :persist => false, :identifiers => tsv.identifiers

    identifiers, persist_input = Misc.process_options options, :identifiers, :persist_input

    if not tsv.fields.include? format
      tsv = tsv.annotate(Hash[*tsv.keys.zip(tsv.values.collect{|l| l.dup}).flatten(1)]) 

      orig_type = tsv.type 
      tsv = tsv.to_double if orig_type != :double

      tsv = tsv.attach identifiers, :fields => [format], :persist_input => true
      tsv = tsv.reorder(format, tsv.fields - [format])

      tsv = tsv.to_flat  if orig_type == :flat

      tsv
    else
      tsv.reorder(format)
    end
  end

  def change_key(*args)
    TSV.change_key(self, *args)
  end

  def self.swap_id(tsv, field, format, options = {})
    options = Misc.add_defaults options, :persist => false, :identifiers => tsv.identifiers

    identifiers, persist_input = Misc.process_options options, :identifiers, :persist

    index = identifiers.index :target => format, :fields => [field], :persist => persist_input

    orig_type = tsv.type 
    tsv = tsv.to_double if orig_type != :double

    pos = tsv.fields.index field
    tsv.with_unnamed do
      if tsv.type == :list or tsv.type == :single
        tsv.through do |k,v|
          v[pos] = index[v[pos]]
          tsv[k] = v
        end
      else
        tsv.through do |k,v|
          v[pos] = index.values_at *v[pos]
          tsv[k] = v
        end
      end
      
      tsv.fields = tsv.fields.collect{|f| f == field ? format : f}
    end

    tsv = tsv.to_flat  if orig_type == :flat

    tsv
  end

  def swap_id(*args)
    TSV.swap_id(self, *args)
  end


end
