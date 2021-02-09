require 'rbbt/workflow/util/orchestrator'
module HPC
  module SLURM

    def self.job_rules(rules, job)
      workflow = job.workflow.to_s
      task_name = job.task_name.to_s
      task_name = job.overriden.to_s if Symbol === job.overriden

      defaults = rules["defaults"] || {}
      defaults = defaults.merge(rules[workflow]["defaults"] || {}) if rules[workflow]

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

    def self.get_job_dependencies(job, job_rules = nil)
      deps = job.dependencies || []
      deps += job.input_dependencies || []
      deps
    end

    def self.get_chains(job, rules, chains = {})
      job_rules = self.job_rules(rules, job)
      job_deps = get_job_dependencies(job)

      input_deps = []
      job.rec_dependencies.each do |dep|
        input_deps.concat dep.input_dependencies
      end

      job_deps.each do |dep|
        input_deps.concat dep.input_dependencies
        get_chains(dep, rules, chains)
      end

      job_deps.select do |dep|
        chained = job_rules["chain_tasks"] &&
          job_rules["chain_tasks"][job.workflow.to_s] && job_rules["chain_tasks"][job.workflow.to_s].include?(job.task_name.to_s)  &&
          job_rules["chain_tasks"][dep.workflow.to_s] && job_rules["chain_tasks"][dep.workflow.to_s].include?(dep.task_name.to_s) 

        dep_skip = ! input_deps.include?(dep) && self.job_rules(rules, dep)["skip"] 
        chained || dep_skip
      end.each do |dep|
        chains[job] ||= [] 
        chains[job] << dep 
        chains[job].concat chains[dep] if chains[dep]
      end

      chains
    end

    def self.workload(job, rules, chains, options, seen = nil)
      if seen.nil?
        seen = {} 
        target_job = true
      end

      job_rules = self.job_rules(rules, job)
      job_deps = get_job_dependencies(job)


      chain = chains[job]
      chain -= seen.keys if chain
      dep_ids = job_deps.collect do |dep|
        seen[dep] = nil if chain && chain.include?(dep) #&& ! job.input_dependencies.include?(dep) 
        ids = workload(dep, rules, chains, options, seen)
        seen[dep] = ids
        ids
      end.compact.flatten.uniq

      return seen[job] || dep_ids if seen.include? job

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


      job_options[:manifest] = chain ? ([job] + chain).uniq.collect{|dep| dep.task_signature} : [job.task_signature]
      if options[:dry_run]
        puts Log.color(:magenta, "Manifest: ") + Log.color(:blue, job_options[:manifest] * ", ") + " - tasks: #{job_options[:task_cpus] || 1} - time: #{job_options[:time]} - config: #{job_options[:config_keys]}"
        puts Log.color(:yellow, "Deps: ") + Log.color(:blue, job_options[:slurm_dependencies]*", ")
        [job.task_signature]
      else
        run_job(job, job_options)
      end
    end


    def self.orchestrate_job(job, options)
      options.delete "recursive_clean"
      options.delete "clean_task"
      options.delete "clean"
      options.delete "tail"
      options.delete "printfile"
      options.delete "detach"

      rules = YAML.load(Open.read(options[:orchestration_rules])) if options[:orchestration_rules]
      rules ||= {}
      IndiferentHash.setup(rules)

      chains = get_chains(job, rules)

      workload(job, rules, chains, options)
    end

  end
end
