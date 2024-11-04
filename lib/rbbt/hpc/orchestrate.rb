#require 'rbbt/workflow/util/orchestrator'
require 'rbbt/hpc/orchestrate/rules'
require 'rbbt/hpc/orchestrate/chains'
require 'rbbt/hpc/orchestrate/batches'
module HPC
  module Orchestration

    def prepare_for_execution(job)
      rec_dependencies = job.rec_dependencies(true)

      return if rec_dependencies.empty?

      all_deps = rec_dependencies + [job]

      all_deps.each do |dep|
        begin
          dep.clean if (dep.error? && dep.recoverable_error?) ||
            dep.aborted? || (dep.done? && dep.updated?)
        rescue RbbtException
          next
        end
      end
    end

    def self.orchestration_rules(orchestration_rules_file = nil)
      rules = {}
      if orchestration_rules_file
        if Open.exists?(orchestration_rules_file)
          rules = Misc.load_yaml(orchestration_rules_file)
        elsif Rbbt.etc.batch[orchestration_rules_file].exists?
          rules = Misc.load_yaml(Rbbt.etc.batch[orchestration_rules_file])
        elsif Rbbt.etc.batch[orchestration_rules_file + '.yaml'].exists?
          rules = Misc.load_yaml(Rbbt.etc.batch[orchestration_rules_file + '.yaml'])
        else
          raise "Orchestration rules file not found: #{orchestration_rules_file}"
        end
      elsif Rbbt.etc.batch["default.yaml"].exists?
        rules = Misc.load_yaml(Rbbt.etc.batch["default.yaml"])
      end

      IndiferentHash.setup(rules)
    end

    def orchestrate_job(job, options)
      options.delete "recursive_clean"
      options.delete "clean_task"
      options.delete "clean"
      options.delete "tail"
      options.delete "printpath"
      options.delete "detach"
      options.delete "jobname"
      options.delete "load_inputs"
      options.delete "provenance"


      Log.high "Prepare for exec"
      prepare_for_execution(job)

      rules = HPC::Orchestration.orchestration_rules(options[:orchestration_rules])

      batches = HPC::Orchestration.job_batches(rules, job)
      Log.high "Compute #{batches.length} batches"

      batch_ids = {}
      last_id = nil
      last_dir = nil
      while batches.any?
        top = batches.select{|b| b[:deps].nil? || (b[:deps].collect{|d| d[:top_level]} - batch_ids.keys).empty? }.first
        raise "No batch without unmet dependencies" if top.nil?
        batches.delete top

        job_options = HPC::Orchestration.merge_rules(options, top[:rules])

        if top[:deps].nil?
          batch_dependencies = [] 
        else 
          top_jobs = top[:jobs]

          batch_dependencies = top[:deps].collect{|d| 
            target = d[:top_level]
            canfail = target.canfail?

            #top_jobs.each do |job|
            #  canfail = true if job.canfail? # job.canfail_paths.include?(target.path)
            #end

            if canfail
              'canfail:' + batch_ids[d[:top_level]].to_s
            else
              batch_ids[d[:top_level]].to_s
            end
          }
        end

        job_options.merge!(:batch_dependencies => batch_dependencies )
        job_options.merge!(:manifest => top[:jobs].collect{|d| d.task_signature })

        if options[:dry_run]
          puts Log.color(:magenta, "Manifest: ") + Log.color(:blue, job_options[:manifest] * ", ") + " - tasks: #{job_options[:task_cpus] || 1} - time: #{job_options[:time]} - config: #{job_options[:config_keys]}"
          puts Log.color(:yellow, "Deps: ") + Log.color(:blue, job_options[:batch_dependencies]*", ")
          puts Log.color(:yellow, "Path: ") + top[:top_level].path
          puts Log.color(:yellow, "Options: ") + job_options.inspect
          batch_ids[top[:top_level]] = top[:top_level].task_signature
        else
          id, dir = run_job(top[:top_level], job_options)
          last_id = batch_ids[top[:top_level]] = id
          last_dir = dir
        end
      end

      [last_id, last_dir]
    end

  end
end
