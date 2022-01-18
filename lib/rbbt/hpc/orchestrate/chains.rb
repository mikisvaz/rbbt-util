module HPC
  module Orchestration
    def self.check_chains(chains, job)
      matches = []
      chains.each do |name, chain|
        next unless chain[:tasks].include?(job.workflow.to_s)
        next unless chain[:tasks][job.workflow.to_s].include?(job.task_name.to_s)
        matches << name
      end
      matches
    end

    def self.parse_chains(rules)
      return {} if rules["chains"].nil?

      chains = IndiferentHash.setup({})
      rules["chains"].each do |name,rules|
        rules  = IndiferentHash.setup(rules.dup)
        chain_tasks = rules.delete(:tasks).split(/,\s*/)
        workflow = rules.delete(:workflow)

        chain_tasks.each do |task|
          chain_workflow, chain_task = task.split("#")
          chain_task, chain_workflow = chain_workflow, workflow if chain_task.nil? or chain_tasks.empty?

          chains[name] ||= IndiferentHash.setup({:tasks => {}, :rules => rules })
          chains[name][:tasks][chain_workflow] ||= []
          chains[name][:tasks][chain_workflow] << chain_task
        end
      end

      chains
    end

    def self.job_dependencies(job)
      (job.dependencies + job.input_dependencies).uniq.select{|d| ! d.done? || d.dirty? }
    end

    #def self.job_workload(job)
    #  workload = []
    #  heap = []
    #  heap << job
    #  while job = heap.pop
    #    next if job.done?
    #    workload << job
    #    heap.concat job_dependencies(job)
    #    heap.uniq!
    #  end
    #  workload.uniq
    #end

    #def self.top_level_job(jobs)
    #  top = jobs.select do |job|
    #    (jobs - job_workload(job)).empty? &&
    #      (job_workload(job) - jobs).select{|j| (job_workload(j) & jobs).any? }.empty?
    #  end
    #  return nil if top.length != 1
    #  top.first
    #end

    #def self.job_chains(rules, job)
    #  workload = job_workload(job)
    #  chains = parse_chains(rules)

    #  chain_jobs = {}
    #  workload.each do |job|
    #    check_chains(chains, job).each do |match|
    #      chain_jobs[match] ||= []
    #      chain_jobs[match] << job
    #    end
    #  end

    #  job_chains = []

    #  seen = []
    #  chain_jobs.sort_by{|name,jobs| jobs.length }.reverse.each do |name,jobs|
    #    remain = jobs - seen
    #    next unless remain.length > 1
    #    top_level_job = top_level_job(jobs)
    #    next if top_level_job.nil?
    #    job_chains << {:jobs => remain, :rules => chains[name][:rules], :top_level_job => top_level_job}
    #    seen.concat remain
    #  end

    #  job_chains
    #end

    #def self._job_chains(rules, job)
    #  workload = job_workload(job)
    #  chains = parse_chains(rules)

    #  matches = check_chains(chains, job)

    #  job_chains = {}
    #  job.dependencies.each do |dep|
    #    dep_chains = _job_chains(rules, dep)
    #    matches.each do |match|
    #      if dep_chains[match] && dep_chains[match].include?(dep)
    #        dep_chains[match].prepend job
    #      end
    #    end
    #    job_chains.merge!(dep_chains)
    #  end

    #  matches.each do |match|
    #    job_chains[match] ||= [job]
    #  end

    #  job_chains
    #end

    #def self.job_chains(rules, job)
    #  job_chains = self._job_chains(rules, job)
    #  iif job_chains
    #  chains = parse_chains(rules)

    #  seen = []
    #  job_chains.collect do |name,jobs|
    #    remain = jobs - seen
    #    next unless remain.length > 1
    #    top_level_job = top_level_job(jobs)
    #    next if top_level_job.nil?
    #    seen.concat remain
    #    {:jobs => remain, :rules => chains[name][:rules], :top_level_job => top_level_job}
    #  end.compact
    #end

    def self.job_chains(rules, job)
      chains = self.parse_chains(rules)

      matches = check_chains(chains, job)

      dependencies = job_dependencies(job)

      job_chains = []
      new_job_chains = {}
      dependencies.each do |dep|
        dep_matches = check_chains(chains, dep)
        common = matches & dep_matches

        dep_chains = job_chains(rules, dep)
        found = []
        dep_chains.each do |match,info|
          if common.include?(match)
            found << match
            new_info = new_job_chains[match] ||= {}
            new_info[:jobs] ||= []
            new_info[:jobs].concat info[:jobs]
            new_info[:top_level] = job
          else
            job_chains << [match, info]
          end
        end

        (common - found).each do |match|
          info = {}
          info[:jobs] = [job, dep]
          info[:top_level] = job
          job_chains << [match, info]
        end
      end

      new_job_chains.each do |match,info|
        info[:jobs].prepend job
        job_chains << [match, info]
      end

      job_chains
    end

  end
end

