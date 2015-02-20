require 'rbbt-util'
require 'rbbt/knowledge_base'

class KnowledgeBase

  class Traverser
    attr_accessor :rules, :assignments, :matches, :kb

    def initialize(kb, rules = [])
      @kb = kb
      @rules = rules
      @assignments = {}
      @matches = {}
    end

    def wildcard(name)
      return name unless is_wildcard?(name)
      assignments[name] || name
    end

    def is_wildcard?(name)
      name[0] == '?'
    end

    def identify(db, source, target)
      source_entities = if is_wildcard? source
                        assignments[source] || :all
                      else
                        kb.identify_source db, source
                      end

      target_entities = if is_wildcard? target
                          assignments[target] || :all
                        else
                          kb.identify_target db, target
                        end

      source_entities = [source_entities] unless Array === source_entities or source_entities == :all
      target_entities = [target_entities] unless Array === target_entities or target_entities == :all

      [source_entities, target_entities]
    end
    
    def reassign(matches, source, target)
      assignments[source] = (matches.any? ? matches.source.uniq : []) if is_wildcard? source
      assignments[target] = (matches.any? ? matches.target.uniq : []) if is_wildcard? target
    end

    def find_paths(rules, all_matches, assignments)
      paths = {}

      rules.zip(all_matches).each do |rule, matches|
        source, db, target = rule.split /\s+/
        if is_wildcard? source
          assigned = assignments[source]
          matches = matches.select{|m| assigned.include? m.source }
        end
        
        if is_wildcard? target
          assigned = assignments[target]
          matches = matches.select{|m| assigned.include? m.target }
        end

        paths[rule] = matches
      end
      paths
    end

    def traverse
      all_matches = []
      rules.each do |rule|
        rule = rule.strip
        next if rule.empty?
        source, db, target, conditions = rule.match(/([^\s]+)\s+([^\s]+)\s+([^\s]+)(?:\s+-\s+([^\s]+))?/).captures

        source_entities, target_entities = identify db, source, target

        matches = kb.subset(db, :source => source_entities, :target => target_entities)
        if conditions
          conditions.split(/\s+/).each do |condition|
            if condition.index "="
              key, value = conditions.split("=")
              matches = matches.select{|m| m.info[key.strip].to_s =~ /\b#{value.strip}\b/}
            else
              matches = matches.select{|m| m.info[condition.strip].to_s =~ /\btrue\b/}
            end
          end
        end

        reassign matches, source, target
        all_matches << matches
      end

      paths = find_paths rules, all_matches, assignments
      
      [assignments, paths]
    end
  end

  def traverse(rules)
    traverser = KnowledgeBase::Traverser.new self, rules
    traverser.traverse
  end
    
end
