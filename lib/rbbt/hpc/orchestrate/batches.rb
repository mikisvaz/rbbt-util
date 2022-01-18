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
        n[:deps] = n[:deps].collect{|b| b[:top_level] }
        iif n
      end
    end

    def self.job_workload(job)
      workload = []
      heap = []
      heap << job
      while job = heap.pop
        next if job.done?
        workload << job
        heap.concat job_dependencies(job)
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
        all_deps = jobs.collect{|d| job_dependencies(d) }.flatten.uniq
        deps = all_deps.collect do |d|
          (batches - [batch]).select{|batch| batch[:jobs].include? d }
        end.flatten.uniq
        batch[:deps] = deps
      end

      batches
    end

    def self.add_rules_and_consolidate(rules, batches)
      chain_rules = parse_chains(rules)

      batches.each do |batch|
        job_rules = batch[:jobs].inject(nil) do |acc,job|
          task_rules = task_specific_rules(rules, job.workflow, job.task_name)
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
          next unless batch[:rules][:skip]
          batch[:rules].delete :skip
          next if batch[:deps].nil?

          if batch[:deps].any?
            target = batch[:deps].select do |target|
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
      job_chains = self.job_chains(rules, job)

      workload = job_workload(job)

      batches = chain_batches(rules, job_chains, workload)
      batches = add_batch_deps(batches)
      batches = add_rules_and_consolidate(rules, batches)
    end
  end
end
