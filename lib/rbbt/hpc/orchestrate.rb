require 'rbbt/workflow/util/orchestrator'
module HPC
  module Orchestration

    def job_rules(rules, job)
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

    def get_job_dependencies(job, job_rules = nil)
      deps = job.dependencies || []
      deps += job.input_dependencies || []
      deps
    end

    def get_recursive_job_dependencies(job)
      deps = get_job_dependencies(job) 
      (deps + deps.collect{|dep| get_recursive_job_dependencies(dep) }).flatten
    end

    def piggyback(job, job_rules, job_deps)
      return false unless job_rules["skip"]
      final_deps = job_deps - job_deps.collect{|dep| get_recursive_job_dependencies(dep)}.flatten.uniq
      final_deps = final_deps.reject{|dep| dep.done? }
      return final_deps.first if final_deps.length == 1
      return false
    end

    def get_chains(job, rules, chains = {})
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

        dep_skip = dep.done? && ! input_deps.include?(dep) && self.job_rules(rules, dep)["skip"] 
        chained || dep_skip
      end.each do |dep|
        chains[job] ||= [] 
        chains[job] << dep 
        chains[job].concat chains[dep] if chains[dep]
      end

      chains
    end

    def workload(job, rules, chains, options, seen = nil)
      return [] if job.done?
      if seen.nil?
        seen = {}
        target_job = true
      end

      job_rules = self.job_rules(rules, job)
      job_deps = get_job_dependencies(job)

      chain = chains[job]
      chain = chain.reject{|j| seen.include? j.path} if chain
      chain = chain.reject{|dep| dep.done? } if chain
      piggyback = piggyback(job, job_rules, job_deps)
      dep_ids = job_deps.collect do |dep|
        seen[dep.path] ||= nil if chain && chain.include?(dep) #&& ! job.input_dependencies.include?(dep) 
        next_options = IndiferentHash.setup(options.dup)
        if piggyback and piggyback == dep
          next_options[:piggyback] ||= []
          next_options[:piggyback].push job
          ids = workload(dep, rules, chains, next_options, seen)
        else
          next_options.delete :piggyback
          ids = workload(dep, rules, chains, next_options, seen)
        end

        ids = [ids].flatten.compact.collect{|id| ['canfail', id] * ":"} if job.canfail_paths.include? dep.path

        seen[dep.path] = ids
        ids
      end.compact.flatten.uniq

      return seen[job.path] || dep_ids if seen.include?(job.path)

      if piggyback and seen[piggyback.path]
        return seen[job.path] = seen[piggyback.path] 
      end

      job_rules.delete :chain_tasks
      job_rules.delete :tasks
      job_rules.delete :workflow
      

      option_config_keys = options[:config_keys]

      job_options = IndiferentHash.setup(options.merge(job_rules).merge(:batch_dependencies => dep_ids))
      job_options.delete :orchestration_rules

      config_keys = job_rules.delete(:config_keys)
      if config_keys
        config_keys.gsub!(/,\s+/,',') 
        job_options[:config_keys] = job_options[:config_keys] ? config_keys + "," + job_options[:config_keys] : config_keys
      end

      if option_config_keys
        option_config_keys = option_config_keys.gsub(/,\s+/,',') 
        job_options[:config_keys] = job_options[:config_keys] ? job_options[:config_keys] + "," + option_config_keys : option_config_keys
      end

      if options[:piggyback]
        manifest = options[:piggyback].uniq
        manifest += [job]
        manifest.concat chain if chain

        job = options[:piggyback].first

        job_rules = self.job_rules(rules, job)
        new_config_keys = self.job_rules(rules, job)[:config_keys]
        if new_config_keys
          new_config_keys = new_config_keys.gsub(/,\s+/,',') 
          job_options[:config_keys] = job_options[:config_keys] ? job_options[:config_keys] + "," + new_config_keys : new_config_keys
        end

        job_options.delete :piggyback
      else
        manifest = [job]
        manifest.concat chain if chain
      end

      manifest.uniq!

      job_options[:manifest] = manifest.collect{|j| j.task_signature }

      job_options[:config_keys] = job_options[:config_keys].split(",").uniq * "," if job_options[:config_keys]

      if options[:dry_run]
        puts Log.color(:magenta, "Manifest: ") + Log.color(:blue, job_options[:manifest] * ", ") + " - tasks: #{job_options[:task_cpus] || 1} - time: #{job_options[:time]} - config: #{job_options[:config_keys]}"
        puts Log.color(:yellow, "Deps: ") + Log.color(:blue, job_options[:batch_dependencies]*", ")
        job_options[:manifest].first
      else
        run_job(job, job_options)
      end
    end


    def orchestrate_job(job, options)
      options.delete "recursive_clean"
      options.delete "clean_task"
      options.delete "clean"
      options.delete "tail"
      options.delete "printpath"
      options.delete "detach"
      options.delete "jobname"

      rules = YAML.load(Open.read(options[:orchestration_rules])) if options[:orchestration_rules]
      rules ||= {}
      IndiferentHash.setup(rules)

      chains = get_chains(job, rules)

      workload(job, rules, chains, options)
    end

  end
end
