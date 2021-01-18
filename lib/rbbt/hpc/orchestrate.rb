require 'rbbt/workflow/util/orchestrator'
module HPC
  module SLURM

    def self.job_rules(rules, job)
      workflow = job.workflow.to_s
      task_name = job.task_name.to_s
      defaults = rules["defaults"] || {}

      job_rules = IndiferentHash.setup(defaults.dup)

      rules["chains"].each do |name,info|
        IndiferentHash.setup(info)
        chain_tasks = info[:tasks].split(/,\s*/)

        chain_tasks.each do |task|
          task_workflow, chain_task = task.split("#")
          chain_task, task_workflow = task_workflow, info[:workflow] if chain_task.nil? or chain_tasks.empty?
          job_rules["chain_tasks"] ||= {}
          job_rules["chain_tasks"][task_workflow] ||= []
          job_rules["chain_tasks"][task_workflow]  << chain_task
          next unless task_name == chain_task.to_s && workflow == task_workflow.to_s
          config_keys = job_rules.delete :config_keys
          job_rules = IndiferentHash.setup(job_rules.merge(info)) 
          if config_keys
            config_keys.gsub!(/,\s+/,',') 
            job_rules[:config_keys] = job_rules[:config_keys] ? config_keys + "," + job_rules[:config_keys] : config_keys
          end
        end

        if job_rules["chain_tasks"][workflow] && job_rules["chain_tasks"][workflow].include?(task_name)
          break
        else
          job_rules.delete "chain_tasks" 
        end
      end if rules["chains"]

      config_keys = job_rules.delete :config_keys
      job_rules = IndiferentHash.setup(job_rules.merge(rules[workflow][task_name])) if rules[workflow] && rules[workflow][task_name]

      if config_keys
        config_keys.gsub!(/,\s+/,',') 
        job_rules[:config_keys] = job_rules[:config_keys] ? config_keys + "," + job_rules[:config_keys] : config_keys
      end

      if rules["skip"] && rules["skip"][workflow]
        job_rules["skip"] = true if rules["skip"][workflow].split(/,\s*/).include? task_name
      end

      job_rules
    end

    def self.get_job_dependencies(job, job_rules)
      deps = job.dependencies || []
      deps += job.input_dependencies || []
      deps
    end

    def self.orchestrate_job(job, options, skip = false, seen = {})
      return if job.done?
      return unless job.path.split("/")[-4] == "jobs"
      seen[:orchestration_target_job] ||= job
      options.delete "recursive_clean"
      options.delete "tail"
      options.delete "printfile"
      rules = YAML.load(Open.read(options[:orchestration_rules])) if options[:orchestration_rules]
      rules ||= {}
      IndiferentHash.setup(rules)

      job_rules = self.job_rules(rules, job)

      deps = get_job_dependencies(job, job_rules)

      dep_ids = deps.collect do |dep|
        skip_dep = job_rules["chain_tasks"] &&
          job_rules["chain_tasks"][job.workflow.to_s] && job_rules["chain_tasks"][job.workflow.to_s].include?(job.task_name.to_s)  &&
          job_rules["chain_tasks"][dep.workflow.to_s] && job_rules["chain_tasks"][dep.workflow.to_s].include?(dep.task_name.to_s) 
        seen[dep.path] ||= self.orchestrate_job(dep, options, skip_dep, seen)
      end.flatten.compact.uniq

      skip = true if job_rules[:skip]
      return dep_ids if skip and seen[:orchestration_target_job] != job

      job_rules.delete :chain_tasks
      job_rules.delete :tasks
      job_rules.delete :workflow
      
      config_keys = job_rules.delete(:config_keys)

      job_options = IndiferentHash.setup(options.merge(job_rules).merge(:slurm_dependencies => dep_ids))
      job_options.delete :orchestration_rules
      if config_keys
        config_keys.gsub!(/,\s+/,',') 
        job_options[:config_keys] = job_options[:config_keys] ? config_keys + "," + job_options[:config_keys] : config_keys
      end

      run_job(job, job_options)
    end
  end
end
