module HPC
  module Orchestration
    def self.check_chains(chains, job)
      return [] if Symbol === job.overriden_task
      matches = []
      chains.each do |name, chain|
        workflow = job.overriden_workflow || job.workflow
        task_name = job.overriden_workflow || job.task_name
        next unless chain[:tasks].include?(workflow.to_s)
        next unless chain[:tasks][workflow.to_s].include?(task_name.to_s)
        matches << name
      end
      matches
    end

    def self.parse_chains(rules)
      chains = IndiferentHash.setup({})

      rules.each do |workflow,rules|
        next unless rules["chains"]
        rules["chains"].each do |name,rules|
          rules  = IndiferentHash.setup(rules.dup)
          chain_tasks = rules.delete(:tasks).split(/,\s*/)
          workflow = rules.delete(:workflow) if rules.include?(:workflow)

          chain_tasks.each do |task|
            chain_workflow, chain_task = task.split("#")
            chain_task, chain_workflow = chain_workflow, workflow if chain_task.nil? or chain_tasks.empty?

            chains[name] ||= IndiferentHash.setup({:tasks => {}, :rules => rules })
            chains[name][:tasks][chain_workflow] ||= []
            chains[name][:tasks][chain_workflow] << chain_task
          end
        end
      end

      return chains if rules["chains"].nil?

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

    def self.job_chains(rules, job, computed = {})
      computed[Misc.fingerprint([rules, job.path, job.object_id])] ||=
        begin
          chains = self.parse_chains(rules)

          matches = check_chains(chains, job)

          dependencies = job_dependencies(job)

          job_chains = []
          new_job_chains = {}
          dependencies.each do |dep|
            dep_matches = check_chains(chains, dep)
            common = matches & dep_matches

            dep_chains = job_chains(rules, dep, computed)
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
end

