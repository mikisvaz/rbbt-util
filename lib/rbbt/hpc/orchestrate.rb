require 'rbbt/workflow/util/orchestrator'
module HPC
  module SLURM
    def self.orchestrate_job(job, options, seen = {})
      return if job.done?
      return unless job.path.split("/")[-4] == "jobs"
      options.delete "recursive_clean"
      options.delete "tail"
      rules = YAML.load(Open.read(options[:rules])) if options[:rules]
      rules ||= {}

      deps = job.dependencies || []
      deps += job.input_dependencies || []

      dep_ids = deps.collect do |dep|
        seen[dep.path] ||= self.orchestrate_job(dep, options.dup, seen)
      end.compact 

      job_rules = Workflow::Orchestrator.job_rules(rules, job)
      job_options = options.merge(job_rules).merge(:slurm_dependencies => dep_ids)
      run_job(job, job_options)
    end
  end
end
