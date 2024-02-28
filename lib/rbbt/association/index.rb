require 'rbbt/tsv'
require 'rbbt/association/open'
require 'rbbt/association/item'

module Association
  def self.index(file, options = nil, persist_options = nil)
    options = options.nil? ? {} : options.dup
    persist_options = persist_options.nil? ?  Misc.pull_keys(options, :persist)  : persist_options.dup 
    persist_options[:serializer] ||= options[:serializer] if options.include?(:serializer)

    persist_options = Misc.add_defaults persist_options.dup, :persist => true, :dir => Rbbt.var.associations
    persist = persist_options[:persist]

    file = version_file(file, options[:namespace]) if options[:namespace] and String === file
    Persist.persist_tsv(file, nil, options, persist_options.merge(:engine => "BDB", :prefix => "Association Index")) do |data|
      options = Misc.add_defaults options.dup, :monitor => "Building index for #{Misc.fingerprint file}"
      recycle = options[:recycle]
      undirected = options[:undirected]

      serializer = persist_options[:serializer] || :list

      persist_options[:file] = persist_options[:file] + '.database' if persist_options[:file]

      database = open(file, options, persist_options.dup.merge(:engine => "HDB"))

      source_field = database.key_field

      fields = database.fields
      target_field = fields.first.split(":").last

      undirected = true if undirected.nil? and source_field == target_field

      key_field = [source_field, target_field, undirected ? "undirected" : nil].compact * "~"

      TSV.setup(data, :key_field => key_field, :fields => fields[1..-1], :type => :list, :serializer => serializer, :namespace => database.namespace)

      data.key_field = key_field
      data.fields = fields[1..-1]
      data.type = :list
      data.serializer ||= serializer
      data.filename ||= file if String === file

      database.with_unnamed do
        database.with_monitor(options[:monitor]) do
          database.through do |source, values|
            case database.type
            when :single
              values = [[values]]
            when :list
              values = values.collect{|v| [v] }
            when :flat
              values = [values]
            end
            next if values.empty?
            next if source.nil? or source.empty?
            next if values.empty?

            #targets, *rest = Misc.zip_fields(Misc.zip_fields(values).uniq)
            
            next if values.first.empty?
            values =  Misc.zip_fields(Misc.zip_fields(values).uniq)
            targets, *rest = values

            size = targets ? targets.length : 0

            rest.each_with_index do |list,i|
              list.replace [list.first] * size if list.length == 1
            end if recycle and size > 1

            rest = Misc.zip_fields rest

            annotations = (Array === rest.first and rest.first.length > 1) ?
              targets.zip(rest) :
              targets.zip(rest * targets.length) 

            source = source.gsub('~','-..-')
            annotations.each do |target, info|
              next if target.nil? or target.empty?
              target = target.gsub('~','-..-')
              key = [source, target] * "~"

              if data[key].nil? or info.nil?
                data[key] = info
              else
                old_info = data[key]
                info = old_info.zip(info).collect{|p| p * ";;" }
                data[key] = info
              end
            end
          end

          if undirected
            new_data = {}

            data.through do |key,values|
              reverse_key = key.split("~").reverse * "~"
              new_data[reverse_key] = values
            end 

            new_data.each do |key,values|
              data[key] = values
            end
          end

        end
      end

      data
    end.tap do |data|
      data.read if not Hash === data and data.respond_to? :read
      Association::Index.setup data
      data.entity_options = options[:entity_options] if options[:entity_options]
      data
    end
  end

  module Index

    attr_accessor :source_field, :target_field, :undirected
    def parse_key_field
      @source_field, @target_field, @undirected = key_field.split("~")
    end

    def self.setup(repo)
      repo.extend Association::Index
      repo.parse_key_field
      repo.unnamed = true
      repo
    end

    def reverse
      @reverse ||= begin
                     if self.respond_to? :persistence_path
                       persistence_path = self.persistence_path
                       persistence_path = persistence_path.find if Path === persistence_path
                       reverse_filename = persistence_path + '.reverse'
                     else
                       raise "Can only reverse a TokyoCabinet::BDB dataset at the time"
                     end

                     if File.exist?(reverse_filename)
                       new = Persist.open_tokyocabinet(reverse_filename, false, serializer, TokyoCabinet::BDB)
                       raise "Index has no info: #{reverse_filename}" if new.key_field.nil?
                       Association::Index.setup new
                       new
                     else
                       FileUtils.mkdir_p File.dirname(reverse_filename) unless File.exist?(File.dirname(reverse_filename))

                       new = Persist.open_tokyocabinet(reverse_filename, true, serializer, TokyoCabinet::BDB)
                       
                       self.with_unnamed do
                         self.with_monitor :desc => "Reversing #{ persistence_path }" do
                           self.through do |key, value|
                             new_key = key.split("~").reverse.join("~")
                             new[new_key] = value
                           end
                         end
                       end
                       annotate(new)
                       new.key_field = key_field.split("~").values_at(1,0,2).compact * "~"
                       new.read_and_close do
                         Association::Index.setup new
                       end
                       new.read
                     end

                     new.unnamed = true

                     new.undirected = undirected

                     new
                   rescue Exception
                     Log.error "Deleting after error reversing database: #{ reverse_filename }"
                     FileUtils.rm reverse_filename if File.exist? reverse_filename
                     raise $!
                   end
    end

    def match(entity)
      return entity.inject([]){|acc,e| acc.concat match(e); acc } if Array === entity
      return [] if entity.nil?
      prefix(entity + "~")
    end

    def matches(entities)
      entities.inject(nil) do |acc,e| 
        m = match(e); 
        if acc.nil? or acc.empty?
          acc = m
        else
          acc.concat m
        end
        acc
      end
    end

    def filter(value_field = nil, target_value = nil, &block)
      if block_given?
        matches = []
        if value_field
          through :key, value_field do |key,values|
            pass = block.call values
            matches << key if pass
          end
        else
          through do |key,values|
            pass = block.call [key, values]
            matches << key if pass
          end
        end
        matches

      else
        matches = []
        if target_value
          target_value = [target_value] unless Array === target_value
          through :key, value_field do |key,values|
            pass = (values & target_value).any?
            matches << key if pass
          end
        else
          through :key, value_field do |key,values|
            pass = false
            values.each do |value|
              pass = true unless value.nil? or value.empty? or value.downcase == 'false'
            end
            matches << key if pass
          end
        end
        matches
      end
    end

    def to_matrix(value_field = nil, &block)
      value_field = fields.first if value_field.nil? and fields.length == 1
      value_pos = identify_field value_field if value_field and String === value_field
      key_field = source_field

      tsv = if value_pos
              AssociationItem.incidence self.keys, key_field do |key|
                if block_given? 
                  yield self[key][value_pos]
                else
                  self[key][value_pos]
                end
              end
            elsif block_given?
              AssociationItem.incidence self.keys, key_field, &block
            else
              AssociationItem.incidence self.keys, key_field 
            end
    end

    #{{{ Subset

    def subset(source, target)
      return [] if source.nil? or target.nil? or source.empty? or target.empty?

      if source == :all or source == "all"
        if target == :all or target == "all"
          return keys
        else
          matches = reverse.subset(target, source)
          return matches.collect{|m| r = m.partition "~"; r.reverse*"" }
        end
      end

      matches = source.uniq.inject([]){|acc,e| 
        if block_given?
          acc.concat(match(e))
        else
          acc.concat(match(e))
        end
      }

      return matches if target == :all or target == "all"

      target_matches = {}

      matches.each{|code| 
        s,sep,t = code.partition "~"
        next if undirected and t > s and source.include? t
        target_matches[t] ||= []
        target_matches[t] << code
      }

      target_matches.values_at(*target.uniq).flatten.compact
    end

  end
end
