#require 'rbbt/workflow/util/orchestrator'
require 'rbbt/hpc/orchestrate/rules'
require 'rbbt/hpc/orchestrate/chains'
require 'rbbt/hpc/orchestrate/batches'
module HPC
  module Orchestration

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

      batches = HPC::Orchestration.job_batches(rules, job)

      batch_ids = {}
      while batches.any?
        top = batches.select{|b| b[:deps].nil? || (b[:deps] - batch_ids.keys).empty? }.first
        raise "No batch without unmet dependencies" if top.nil?
        batches.delete top
        job_options = options.merge(top[:rules])

        if top[:deps].nil?
          batch_dependencies = [] 
        else 
          top_jobs = top[:jobs]

          batch_dependencies = top[:deps].collect{|d| 
            target = d[:top_level]
            canfail = false

            top_jobs.each do |job|
              canfail = true if job.canfail_paths.include?(target.path)
            end

            if canfail
              'canfail:' + batch_ids[d].to_s
            else
              batch_ids[d].to_s
            end
          }
        end

        job_options.merge!(:batch_dependencies => batch_dependencies )
        job_options.merge!(:manifest => top[:jobs].collect{|d| d.task_signature })

        if options[:dry_run]
          puts Log.color(:magenta, "Manifest: ") + Log.color(:blue, job_options[:manifest] * ", ") + " - tasks: #{job_options[:task_cpus] || 1} - time: #{job_options[:time]} - config: #{job_options[:config_keys]}"
          puts Log.color(:yellow, "Deps: ") + Log.color(:blue, job_options[:batch_dependencies]*", ")
          batch_ids[top] = top[:top_level].task_signature
        else
          id = run_job(top[:top_level], job_options)
          batch_ids[top] = id
        end
      end
    end

  end
end
