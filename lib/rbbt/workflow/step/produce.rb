class Step
  def self.produce_dependencies(jobs, tasks, cpus)
    deps = []

    jobs = [jobs] unless Array === jobs
    tasks = [tasks] unless Array === tasks
    tasks = tasks.collect{|t| t.to_s}

    jobs.each do |job|
      job.rec_dependencies.each do |dep|
        next if dep.done?
        dep.clean if dep.error? && dep.recoverable_error?
        deps << dep if tasks.include?(dep.task_name.to_s) or tasks.include?([dep.workflow.to_s, dep.task_name] * "#")
      end
    end

    cpus = jobs.length if cpus.to_s == "max"
    cpus = cpus.to_i if String === cpus
    TSV.traverse deps.collect{|dep| dep.path}, :type => :array, :cpus => cpus, :bar => "Prepare dependencies #{Misc.fingerprint tasks} for #{Misc.fingerprint jobs}" do |path|
      dep = deps.select{|dep| dep.path == path}.first
      dep.produce
      nil
    end
  end
end
