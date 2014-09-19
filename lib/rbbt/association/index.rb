require 'rbbt/tsv'
module Association
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
                     persistence_path = self.persistence_path
                     persistence_path = persistence_path.find if Path === persistence_path
                     reverse_filename = persistence_path + '.reverse'

                     if File.exists?(reverse_filename)
                       new = Persist.open_tokyocabinet(reverse_filename, false, serializer, TokyoCabinet::BDB)
                       new
                     else
                       FileUtils.mkdir_p File.dirname(reverse_filename) unless File.exists?(File.basename(reverse_filename))
                       new = Persist.open_tokyocabinet(reverse_filename, true, serializer, TokyoCabinet::BDB)
                       new.write
                       through do |key, value|
                         new_key = key.split("~").reverse.join("~")
                         new[new_key] = value
                       end
                       annotate(new)
                       new.key_field = key_field.split("~").values_at(1,0,2).compact * "~"
                       new.read
                     end

                     new.unnamed = true

                     Association::Index.setup new

                     new.undirected = undirected

                     new
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

    #{{{ Subset

    def select_entities(entities)
      source_type = Entity.formats[source_field] 
      target_type = Entity.formats[target_field]

      source_entities = entities[:source] || entities[source_field] || entities[Entity.formats[source_field].to_s] 
      target_entities = entities[:target] || entities[target_field] || entities[Entity.formats[target_field].to_s]

      [source_entities, target_entities]
    end

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
        next if undirected and t > s 
        target_matches[t] ||= []
        target_matches[t] << code
      }

      target_matches.values_at(*target.uniq).flatten.compact
    end

    def subset_entities(entities)
      source, target = select_entities(entities)
      return [] if source.nil? or target.nil?
      return [] if Array === target and target.empty?
      return [] if Array === source and source.empty?
      subset source, target
    end
  end
end
