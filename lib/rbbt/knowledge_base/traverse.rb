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

    def clean_matches(rules, all_matches, assignments)
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

    def _fp(rules, clean_matches, assignments)
      return true if rules.empty?

      rule, *rest = rules
      source, db, target = rule.split /\s+/

      paths = {}
      matches = clean_matches[rule]
      Annotated.purge(matches).each do |match|
        new_assignments = nil
        match_source, _sep, match_target = match.partition "~"

        if is_wildcard? source
          next if assignments[source] and assignments[source]  != match_source
          new_assignments ||= assignments.dup
          new_assignments[source] = match_source
        end

        if is_wildcard? target
          next if assignments[target] and assignments[target]  != match_target
          new_assignments ||= assignments.dup
          new_assignments[target] = match_target
        end

        new_paths = _fp(rest, clean_matches, new_assignments)
        next unless new_paths
        paths[match] = new_paths
      end

      return false if paths.empty?

      paths 
    end

    def _ep(paths)
      found = []
      paths.each do |match,_next|
        case _next
        when TrueClass
          found << [match]
        when FalseClass
          next
        else
          _ep(_next).each do |_n|
            found << [match] + _n
          end
        end
      end
      found
    end

    def find_paths(rules, all_matches, assignments)
      clean_matches = clean_matches(rules, all_matches, assignments)

      path_hash = _fp(rules, clean_matches, {})

      return [] unless path_hash
      _ep(path_hash).collect do |path|
        path.zip(clean_matches.values_at(*rules)).collect do |item, matches|
          matches.first.annotate item.dup
        end
      end
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
