require 'rbbt/hpc/orchestrate/rules'
require 'rbbt/hpc/orchestrate/chains'

module HPC
  module Orchestration

    def self.pb(batch)
      if Array === batch
        iii :BATCHES
        batch.each{|b| pb b}
        iii :END_BATCHES
      else
        n = batch.dup
        n[:deps] = n[:deps].collect{|b| b[:top_level] } if n[:deps]
        iif n
      end
    end

    def self.job_workload(job)
      workload = []
      path_jobs = {}

      path_jobs[job.path] = job

      heap = []
      heap << job.path
      while job_path = heap.pop
        job = path_jobs[job_path]
        next if job.done?
        workload << job

        deps =  job_dependencies(job)

        deps.each do  |d|
          path_jobs[d.path] ||= d
        end

        heap.concat deps.collect(&:path)
        heap.uniq!
      end
      workload.uniq
    end


    def self.chain_batches(rules, chains, workload)
      chain_rules = parse_chains(rules)

      batches = []
      while job = workload.pop
        matches = chains.select{|name,info| info[:jobs].include? job }
        if matches.any?
          name, info = matches.sort_by do |name,info|
            num_jobs = info[:jobs].length
            total_tasks = chain_rules[name][:tasks].values.flatten.uniq.length
            num_jobs.to_f + 1/total_tasks
          end.last
          workload = workload - info[:jobs]
          info[:chain] = name
          batch = info
        else
          batch = {:jobs => [job], :top_level => job}
        end

        chains.delete_if{|name,info| batch[:jobs].include? info[:top_level] }

        chains.each do |name,info|
          info[:jobs] = info[:jobs] - batch[:jobs]
        end

        chains.delete_if{|name,info| info[:jobs].length < 2 }

        batches << batch
      end

      batches
    end

    def self.add_batch_deps(batches)

      batches.each do |batch|
        jobs = batch[:jobs]
        all_deps = jobs.collect{|d| job_dependencies(d) }.flatten.uniq - jobs

        minimum = all_deps
        all_deps.each do |dep|
          minimum -= job_dependencies(dep)
        end

        all_deps = minimum 
        deps = all_deps.collect do |d|
          (batches - [batch]).select{|batch| batch[:jobs].collect(&:path).include? d.path }
        end.flatten.uniq
        batch[:deps] = deps
      end

      batches
    end

    def self.add_rules_and_consolidate(rules, batches)
      chain_rules = parse_chains(rules)

      batches.each do |batch|
        job_rules = batch[:jobs].inject(nil) do |acc,job|
          workflow = job.workflow
          task_name = job.task_name
          task_rules = task_specific_rules(rules, workflow, task_name)
          acc = accumulate_rules(acc, task_rules.dup)
        end

        if chain = batch[:chain]
          batch[:rules] = merge_rules(chain_rules[chain][:rules].dup, job_rules)
        else
          batch[:rules] = job_rules
        end
      end

      begin
        batches.each do |batch|
          batch[:deps] = batch[:deps].collect do |dep|
            dep[:target] || dep
          end if batch[:deps]
        end

        batches.each do |batch|
          next if batch[:top_level].overriden?
          next unless batch[:rules][:skip]
          batch[:rules].delete :skip
          next if batch[:deps].nil?

          if batch[:deps].any?
            batch_dep_jobs = batch[:top_level].rec_dependencies
            target = batch[:deps].select do |target|
              batch_dep_jobs.include?(target[:top_level]) && # Don't piggyback batches that are an input dependency, only real dependencies
                (batch[:deps] - [target] - target[:deps]).empty?
            end.first
            next if target.nil?
            target[:jobs] = batch[:jobs] + target[:jobs]
            target[:deps] = (target[:deps] + batch[:deps]).uniq - [target]
            target[:top_level] = batch[:top_level]
            target[:rules] = accumulate_rules(target[:rules], batch[:rules])
            batch[:target] = target
          end
          raise TryAgain
        end
      rescue TryAgain
        retry
      end

      batches.delete_if{|b| b[:target] } 

      batches
    end

    def self.job_batches(rules, job)
      job_chains = self.job_chains(rules, job).dup

      workload = job_workload(job).uniq

      batches = chain_batches(rules, job_chains, workload)
      batches = add_batch_deps(batches)
      batches = add_rules_and_consolidate(rules, batches)
    end
  end
end
