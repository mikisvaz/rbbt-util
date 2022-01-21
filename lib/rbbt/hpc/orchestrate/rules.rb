module HPC
  module Orchestration
    def self.add_config_keys(current, new)
      if current.nil?
        new
      else
        new + ',' + current
      end.gsub(/,\s*/,',').split(",").reverse.uniq.reverse * ","
    end

    def self.workflow_rules(rules, workflow)
      return {} if rules[workflow].nil?
      return {} if rules[workflow]["defaults"].nil? 
      IndiferentHash.setup(rules[workflow]["defaults"])
    end

    def self.merge_rules(current, new)
      return IndiferentHash.setup({}) if (new.nil? || new.empty?) && (current.nil? || current.empty?)
      return IndiferentHash.setup(current.dup) if new.nil? || new.empty?
      return IndiferentHash.setup(new.dup) if current.nil? || current.empty?
      target = IndiferentHash.setup(current.dup)
      new.each do |k,value|
        case k.to_s
        when "config_keys"
          target[k] = add_config_keys target["config_keys"], value
        else
          next if target.include?(k)
          target[k] = value
        end
      end
      target
    end

    def self.accumulate_rules(current, new)
      return IndiferentHash.setup({}) if (new.nil? || new.empty?) && (current.nil? || current.empty?)
      return IndiferentHash.setup(current.dup) if new.nil? || new.empty?
      return IndiferentHash.setup(new.dup) if current.nil? || current.empty?
      target = IndiferentHash.setup(current.dup)
      new.each do |k,value|
        case k.to_s
        when "config_keys"
          target[k] = add_config_keys target["config_keys"], value
        when "cpus"
          target[k] = [target[k], value].compact.sort_by{|v| v.to_i}.last
        when "time"
          target[k] = Misc.format_seconds [target[k], value].compact.inject(0){|acc,t|  acc += Misc.timespan t }
        when "skip"
          skip = target[k] && value
          target.delete k unless skip
        else
          next if target.include?(k)
          target[k] = value
        end
      end
      target
    end

    def self.task_specific_rules(rules, workflow, task)
      defaults = rules[:defaults] || {}
      workflow = workflow.to_s
      task = task.to_s
      return defaults if rules[workflow].nil?
      workflow_rules = merge_rules(workflow_rules(rules, workflow), defaults)
      return IndiferentHash.setup(workflow_rules.dup) if rules[workflow][task].nil?
      merge_rules(rules[workflow][task], workflow_rules)
    end


  end
end
