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
    end

    def reverse
      @reverse ||= begin
                     reverse_filename = persistence_path + '.reverse'

                     if File.exists?(reverse_filename)
                       new = Persist.open_tokyocabinet(reverse_filename, false, serializer, TokyoCabinet::BDB)
                     else
                       new = Persist.open_tokyocabinet(reverse_filename, true, serializer, TokyoCabinet::BDB)
                       new.write
                       through do |key, value|
                         new_key = key.split("~").reverse.join("~")
                         new[new_key] = value
                       end
                       annotate(new)
                       new.key_field = key_field.split("~").values_at(1,0,2).compact * "~"
                       new.close
                     end

                     new.unnamed = true
                     new
                   end
    end

    def match(entity)
      prefix(entity + "~")
    end

    def neighbours(entity)
      return [] if entity.nil? 
      if undirected
        match(entities)
      else
        [match(entities), reverse.match(entities)].
          inject(nil){|acc,e| acc = acc.nil? ? e : (e.nil? ? acc : acc.concat(e)) }
      end
    end

    def subset(source, target)
      return [] if source.nil? or source.empty? or target.nil? or target.empty?
      matches = source.inject([]){|acc,e| acc = acc.concat match(e) }
      matches.delete_if{|code| 
        s, t = code.split("~")
        t > s and undirected or not target.include? t
      }
      matches
    end

    def select_entities(entities)
      source_type = Entity.formats[source_field] || source_field
      target_type = Entity.formats[target_field] || target_field

      source_entities = entities[source_type.to_s] 
      target_entities = entities[target_type.to_s]

      [source_entities, target_entities]
    end

    def subset_entities(entities)
      source, target = select_entities(entities)
      subset source, target
    end

    def annotate(entities, type)
      Misc.prepare_entity(entities, target_type, Entity.format[target_type])
    end

    def children(entity)
      children = match(entity).collect{|e| e.split("~") }
    end

    def parents(entity)
      reverse.match(entity).collect{|e| e.split("~") }
    end

    def neighbours(entitiy)
      if undirected
        children
      else
        children + parents
      end
    end


    #{{{ Work
    #def connections(entities)
    #  source_field, target_field, undirected = key_field.split("~")

    #  source_type = Entity.formats[source_field] || source_field
    #  target_type = Entity.formats[target_field] || target_field

    #  source_entities = entities[source_type.to_s] 
    #  target_entities = entities[target_type.to_s]

    #  return [] if source_entities.nil? or target_entities.nil?

    #  source_entities.collect do |entity|
    #    keys = prefix(entity + "~")
    #    keys.collect do |key|
    #      source, target = key.split("~")
    #      next unless target_entities.include? target
    #      next if undirected and target > source
    #      info = Hash[*fields.zip(self[key]).flatten]

    #      {:source => source, :target => target, :info => info}
    #    end.compact
    #  end.flatten
    #end

    #def children(entity)
    #  return [] if entity.nil?
    #  match(entity).split("~").last
    #end

    #def children(source_entities)
    #  return [] if source_entities.nil?
    #  source_entities.collect do |source|
    #    keys = prefix(source + "~")
    #    keys.collect do |key|
    #      source, target = key.split("~")
    #      target
    #    end.compact
    #  end.flatten.uniq
    #end

    #def parents(target_entities)
    #  return [] if target_entities.nil?
    #  rev_repo = reverse 
    #  rev_repo.children(target_entities)
    #end

    #def neighbours(entities)
    #  return [] if entities.nil? or entities.empty?
    #  source_field, target_field, undirected = key_field.split("~")
    #  if undirected
    #    children(entities)
    #  else
    #    [children(entities), parents(entities)].
    #      inject(nil){|acc,e| acc = acc.nil? ? e : (e.nil? ? acc : acc.concat(e)) }
    #  end
    #end

    #def connections(entities)
    #  source_field, target_field, undirected = key_field.split("~")

    #  source_type = Entity.formats[source_field] || source_field
    #  target_type = Entity.formats[target_field] || target_field

    #  source_entities = entities[source_type.to_s] 
    #  target_entities = entities[target_type.to_s]

    #  return [] if source_entities.nil? or target_entities.nil?

    #  source_entities.collect do |entity|
    #    keys = prefix(entity + "~")
    #    keys.collect do |key|
    #      source, target = key.split("~")
    #      next unless target_entities.include? target
    #      next if undirected and target > source
    #      info = Hash[*fields.zip(self[key]).flatten]

    #      {:source => source, :target => target, :info => info}
    #    end.compact
    #  end.flatten
    #end

    #def children(source_entities)
    #  return [] if source_entities.nil?
    #  source_entities.collect do |source|
    #    keys = prefix(source + "~")
    #    keys.collect do |key|
    #      source, target = key.split("~")
    #      target
    #    end.compact
    #  end.flatten.uniq
    #end

    #def parents(target_entities)
    #  return [] if target_entities.nil?
    #  rev_repo = reverse
    #  rev_repo.children(target_entities)
    #end

    #def neighbours(entities)
    #  return [] if entities.nil? or entities.empty?
    #  source_field, target_field, undirected = key_field.split("~")
    #  if undirected
    #    children(entities)
    #  else
    #    [children(entities), parents(entities)].
    #      inject(nil){|acc,e| acc = acc.nil? ? e : (e.nil? ? acc : acc.concat(e)) }
    #  end
    #end

  end
end
