require 'rbbt/tsv'
require 'rbbt/persist'


module TSV
  def self.change_key(tsv, format, options = {}, &block)
    options = Misc.add_defaults options, :persist => false, :identifiers => tsv.identifiers

    identifiers, persist_input = Misc.process_options options, :identifiers, :persist_input

    identifiers = Organism.identifiers(tsv.namespace) if identifiers.nil? and tsv.namespace

    if not tsv.fields.include? format
      new = {}
      tsv.each do |k,v|
        new[k] = v.dup
      end
      orig_fields = tsv.fields
      tsv = tsv.annotate new
      new.fields = new.fields.collect{|f| "TMP-" << f }

      orig_type = tsv.type 
      tsv = tsv.to_double if orig_type != :double

      if Array === identifiers
        tsv = tsv.attach identifiers.first, :fields => [format], :persist_input => true, :identifiers => identifiers.last
      else
        tsv = tsv.attach identifiers, :fields => [format], :persist_input => true
      end

      tsv = tsv.reorder(format, tsv.fields[0..-2])

      tsv = tsv.to_flat  if orig_type == :flat

      tsv = tsv.to_list(&block)  if orig_type == :list

      tsv.fields = orig_fields

      tsv
    else
      tsv.reorder(format)
    end
  end

  def change_key(format, options = {}, &block)
    options = Misc.add_defaults options, :identifiers => self.identifiers
    TSV.change_key(self, format, options, &block)
  end

  def self.swap_id(tsv, field, format, options = {}, &block)
    options = Misc.add_defaults options, :persist => false, :identifiers => tsv.identifiers, :compact => true

    identifiers, persist_input, compact = Misc.process_options options, :identifiers, :persist, :compact

    fields = identifiers.all_fields.include?(field)? [field] : nil
    index = identifiers.index :target => format, :fields => fields, :persist => persist_input

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
          _values = index.values_at(*v[pos])
          _values.compact! if compact
          v[pos] = _values
          tsv[k] = v
        end
      end
      
      tsv.fields = tsv.fields.collect{|f| f == field ? format : f}
    end

    tsv = tsv.to_flat  if orig_type == :flat

    tsv = tsv.to_list(&block)  if orig_type == :list

    tsv
  end

  def swap_id(*args)
    TSV.swap_id(self, *args)
  end

  def self.translation_index(files, target = nil, source = nil, options = {})
    return nil if source == target
    options = Misc.add_defaults options.dup, :persist => true

    target = Entity.formats.find(target) if Entity.formats.find(target)
    source = Entity.formats.find(source) if Entity.formats.find(source)
    fields = (source and not source.empty?) ? [source] : nil

    files.each do |file|
      if TSV === file
        all_fields = file.all_fields
        target = file.fields.first if target.nil?
        if (source.nil? or all_fields.include? source) and all_fields.include? target
          return file.index(options.merge(:target => target, :fields => fields, :order => true)) 
        end
      else
        next unless file.exists?
        begin
          all_fields = TSV.parse_header(file).all_fields
          target = all_fields[1] if target.nil?
          if (source.nil? or all_fields.include? source) and all_fields.include? target
            index = TSV.index(file, options.merge(:target => target, :fields => fields, :order => true)) 
            return index
          end
        rescue Exception
          Log.exception $!
          Log.error "Exception reading identifier file: #{file.find}"
        end
      end
    end

    files.each do |file|
      all_fields = TSV === file ? file.all_fields : TSV.parse_header(file).all_fields 

      files.each do |other_file|
        next if file == other_file

        other_all_fields = TSV === other_file ? other_file.all_fields : TSV.parse_header(other_file).all_fields 

        common_field = (all_fields & other_all_fields).first

        if common_field and (source.nil? or source.empty? or all_fields.include? source) and other_all_fields.include? target 

          index = Persist.persist_tsv(nil, Misc.fingerprint(files), {:files => files, :source => source, :target => target}, :prefix => "Translation index", :persist => options[:persist]) do |data|

            index = TSV === file ? 
              file.index(options.merge(:target => common_field, :fields => fields)) :
              TSV.index(file, options.merge(:target => common_field, :fields => fields))

            other_index = TSV === other_file ? 
              other_file.index(options.merge(:target => target, :fields => [common_field])) :
              TSV.index(other_file, options.merge(:target => target, :fields => [common_field]))

            data.serializer = :clean
            
            # ToDo: remove the need to to the `to_list` transformation
            data.merge! index.to_list.attach(other_index.to_list).slice([target]).to_single
          end
          return index
        end
      end
    end
    return nil
  end

  def self.translate(tsv, *args)
    new = TSV.open translate_stream(tsv, *args)
    new.identifiers = tsv.identifiers
    new
  end

  def self.translate_stream(tsv, field, format, options = {}, &block)
    options = Misc.add_defaults options, :persist => false, :identifier_files => tsv.identifier_files, :compact => true

    identifier_files, identifiers, persist_input, compact = Misc.process_options options, :identifier_files, :identifiers, :persist, :compact
    identifier_files = [tsv, identifiers].compact if identifier_files.nil? or identifier_files.empty?

    identifier_files.uniq!

    index = translation_index identifier_files, format, field, options.dup
    raise "No index: #{Misc.fingerprint([identifier_files, field, format])}" if index.nil?

    orig_type = tsv.type 
    tsv = tsv.to_double if orig_type != :double

    pos = tsv.identify_field field

    new_options = tsv.options
    new_options[:identifiers] = tsv.identifiers.find if tsv.identifiers

    case pos
    when :key
      new_options[:key_field] = format if tsv.key_field == field
      dumper = TSV::Dumper.new new_options
      dumper.init
      TSV.traverse tsv, :into => dumper do |key,values|
        new_key = index[key]
        [new_key, values]
      end
    else
      new_options[:fields] = tsv.fields.collect{|f| f == field ? format : f }
      dumper = TSV::Dumper.new new_options
      dumper.init

      case tsv.type
      when :double
        TSV.traverse tsv, :into => dumper do |key,values|
          original = values[pos]
          new = index.values_at *original
          values[pos] = new
          [key, values]
        end
      when :list
        TSV.traverse tsv, :into => dumper do |key,values|
          original = values[pos]
          new = index[original]
          values[pos] = new
          [key, values]
        end
      when :flat
        TSV.traverse tsv, :into => dumper do |key,values|
          new = index.values_at *values
          [key, new]
        end
      when :single
        TSV.traverse tsv, :into => dumper do |key,original|
          new = index[original]
          [key, new]
        end
      end
    end

    dumper.stream
  end
end
