
class Step

  ##STREAM_CACHE = {}
  ##STREAM_CACHE_MUTEX = Mutex.new
  ##def self.purge_stream_cache
  ##  Log.medium "Purging dup. stream cache"
  ##  STREAM_CACHE_MUTEX.synchronize do
  ##    #STREAM_CACHE.collect{|k,s| 
  ##    #  Thread.new do
  ##    #    Misc.consume_stream s
  ##    #  end
  ##    #}
  ##    STREAM_CACHE.clear
  ##  end
  ##end

  ##def self.dup_stream(stream)
  ##  case stream
  ##  when IO, File, Step
  ##    return stream if stream.respond_to?(:closed?) and stream.closed?
  ##    return stream if stream.respond_to?(:done?) and stream.done?

  ##    STREAM_CACHE_MUTEX.synchronize do
  ##      stream_key = Misc.fingerprint(stream)
  ##      current = STREAM_CACHE[stream_key]
  ##      case current
  ##      when nil
  ##        Log.medium "Not duplicating stream #{stream_key}"
  ##        STREAM_CACHE[stream_key] = stream
  ##      when File
  ##        if Open.exists? current.path 
  ##          Log.medium "Reopening file #{stream_key}"
  ##          Open.open(current.path)
  ##        else
  ##          new = Misc.dup_stream(current)
  ##          Log.medium "Duplicating file #{stream_key} #{current.inspect} => #{Misc.fingerprint(new)}"
  ##          new
  ##        end
  ##      when Step
  ##        job = current
  ##        current = job.result 
  ##        new = Misc.dup_stream(current)
  ##        job.result = current
  ##        Log.medium "Duplicating step #{stream_key} #{current.inspect} => #{Misc.fingerprint(new)}"
  ##        new
  ##      else
  ##        new = Misc.dup_stream(current)
  ##        Log.medium "Duplicating stream #{stream_key} #{ Misc.fingerprint(stream) } => #{Misc.fingerprint(new)}"
  ##        new
  ##      end
  ##    end
  ##  when TSV::Dumper#, TSV::Parser
  ##    stream = stream.stream
  ##    return stream if stream.closed?

  ##    STREAM_CACHE_MUTEX.synchronize do
  ##      if STREAM_CACHE[stream].nil?
  ##        Log.high "Not duplicating dumper #{ stream.inspect }"
  ##        STREAM_CACHE[stream] = stream
  ##      else
  ##        new = Misc.dup_stream(STREAM_CACHE[stream])
  ##        Log.high "Duplicating dumper #{ stream.inspect } into #{new.inspect}"
  ##        new
  ##      end
  ##    end
  ##  else
  ##    stream
  ##  end
  ##end

  def self.prepare_for_execution(job)
    return if (job.done? and not job.dirty?) or 
    (job.streaming? and job.running?) or 
    (defined? WorkflowRESTClient and WorkflowRESTClient::RemoteStep === job and not (job.error? or job.aborted?))

    job.clean if job.error? or job.aborted? or (job.started? and not job.running? and not job.error?)

    raise DependencyError, job if job.error?
  end

  def log_dependency_exec(dependency, action)
    task_name = self.task_name

    str = Log.color(:reset, "")
    str << Log.color(:yellow, task_name.to_s || "") 
    str << " "
    str << Log.color(:magenta, action.to_s)
    str << " "
    str << Log.color(:yellow, dependency.task_name.to_s || "")
    str << " -- "
    str << "#{Log.color :blue, dependency.path}"

    Log.info str
  end

  def execute_dependency(dependency, log = true)
    task_name = self.task_name
    begin

      dependency.resolve_input_steps

      if dependency.done?
        log_dependency_exec(dependency, :done) if log
        return
      end

      if not dependency.started?
        log_dependency_exec(dependency, :starting)
        dependency.run(:stream)
        raise TryAgain
      end

      dependency.grace

      if dependency.aborted?
        log_dependency_exec(dependency, "aborted (clean)")
        dependency.clean
        raise TryAgain
      end

      if dependency.error?
        log_dependency_exec(dependency, :error)
        raise DependencyError, [dependency.path, dependency.messages.last] * ": " if dependency.error?
      end

      if dependency.streaming?
        log_dependency_exec(dependency, :streaming) if log
        return
      end

      begin
        log_dependency_exec(dependency, :joining)
        dependency.join
        raise TryAgain unless dependency.done?
      rescue Aborted
        raise TryAgain
      end

    rescue TryAgain
      retry
    rescue Aborted, Interrupt
      Log.error "Aborted dep. #{Log.color :red, dependency.task_name.to_s}"
      raise $!
    rescue Interrupt
      Log.error "Interrupted while in dep. #{Log.color :red, dependency.task_name.to_s}"
      raise $!
    rescue Exception
      Log.error "Exception in dep. #{ Log.color :red, dependency.task_name.to_s }"
      Log.exception $!
      raise $!
    end
  end

  #def dup_inputs
  #  return if true or @dupped or ENV["RBBT_NO_STREAM"] == 'true'
  #  Log.low "Dupping inputs for #{path}"
  #  dupped_inputs = @inputs.collect do |input|
  #    Step.dup_stream input
  #  end
  #  @inputs.replace dupped_inputs
  #  @dupped = true
  #end

  def consolidate_dependencies(path_deps = {})
    return false if @consolidated  or dependencies.nil? or dependencies.empty?
    consolidated_deps = dependencies.collect do |dep|
      dep.consolidate_dependencies(path_deps)
      path = dep.path
      path_deps[path] ||= dep
    end
    dependencies.replace consolidated_deps
    @consolidated = true
  end

  #def prepare_dependencies
  #  dep_step = {}

  #  all_deps = rec_dependencies + [self]

  #  seen_paths = Set.new
  #  all_deps.uniq.each do |step|
  #    next if seen_paths.include? step.path
  #    Step.prepare_for_execution(step)
  #    seen_paths << step.path
  #    step.dependencies.each do |step_dep|
  #      dep_step[step_dep.path] ||= []
  #      dep_step[step_dep.path] << step_dep
  #    end
  #  end

  #  seen_paths = Set.new
  #  rec_dependencies.uniq.each do |step|
  #    next if seen_paths.include? step.path
  # = {}    seen_paths << step.path
  #    execute_dependency(step)
  #    if step.streaming? and step.result
  #      if dep_step[step.path] and dep_step[step.path].length > 1
  #        stream = step.result
  #        other_steps = dep_step[step.path] - [step]
  #        copies = Misc.dup_stream_multiple(stream, other_steps.length)
  #        other_steps.zip(copies).each do |other,dupped_stream|
  #          other.instance_variable_set(:@result, dupped_stream)
  #        end
  #      end
  #    end
  #  end

  #  Step.purge_stream_cache
  #end

  def execute_and_dup(step, dep_step, log = true)
    dup = ! step.result
    execute_dependency(step, log)
    if dup and step.streaming? and step.result
      if dep_step[step.path] and dep_step[step.path].length > 1
        stream = step.result
        other_steps = dep_step[step.path] - [step]
        copies = Misc.dup_stream_multiple(stream, other_steps.length)
        log_dependency_exec(step, "duplicating #{copies.length}") 
        other_steps.zip(copies).each do |other,dupped_stream|
          other.instance_variable_set(:@result, dupped_stream)
        end
      end
    end
  end

  def run_compute_dependencies(type, list, dep_step = {})
    if Array === type
      type, *rest = type
    end

    case type
    when :produce, :no_dup
      list.each do |step|
        step.produce
        nil
      end
    when :bootstrap
      cpus = rest.nil? ? nil : rest.first 
      cpus = 30 if cpus.nil?
      cpus = list.length / 2 if cpus > list.length / 2

      Misc.bootstrap(list, cpus, :bar => "Bootstrapping dependencies for #{path}", :_respawn => :always) do |dep|
        dep.produce
        nil
      end
    else
      list.each do |step|
        execute_and_dup(step, dep_step, false)
      end
    end
  end

  def run_dependencies
    dep_step = {}

    all_deps = rec_dependencies + [self]

    dependencies.each do |dep|
      next unless ComputeDependency === dep
      if dep.compute == :produce
        dep.produce 
      end
    end

    compute_deps = rec_dependencies.collect do |dep|
      next unless ComputeDependency === dep
      dep.rec_dependencies
    end.compact.flatten.uniq

    seen_paths = Set.new
    all_deps.uniq.each do |step|
      next if seen_paths.include? step.path
      seen_paths << step.path
      Step.prepare_for_execution(step) unless step == self
      next unless step.dependencies and step.dependencies.any?
      step.dependencies.each do |step_dep|
        next if step_dep.done? or step_dep.running? or (ComputeDependency === step_dep and step_dep.compute == :nodup)
        dep_step[step_dep.path] ||= []
        dep_step[step_dep.path] << step_dep
      end
    end

    required_dep_paths = []
    dep_step.each do |path,list|
      required_dep_paths << path if list.length > 1
    end

    required_dep_paths.concat dependencies.collect{|dep| dep.path }

    log :dependencies, "Dependencies for step #{Log.color :yellow, task.name.to_s || ""}"

    pre_deps = []
    compute_pre_deps = {}
    last_deps = []
    compute_last_deps = {}
    seen_paths = Set.new
    rec_dependencies.uniq.each do |step| 
      next if seen_paths.include? step.path
      seen_paths << step.path
      next unless required_dep_paths.include? step.path
      if dependencies.include?(step) and step.inputs.flatten.select{|i| Step === i}.any?
        if ComputeDependency === step
          compute_last_deps[step.compute] ||= []
          compute_last_deps[step.compute] << step
        else
          last_deps << step
        end
      else
        if ComputeDependency === step
          compute_pre_deps[step.compute] ||= []
          compute_pre_deps[step.compute] << step
        else
          pre_deps << step #if dependencies.include?(step)
        end
      end
    end

    pre_deps.each do |step|
      next if compute_deps.include? step
      execute_and_dup(step, dep_step, false)
    end

    compute_pre_deps.each do |type,list|
      run_compute_dependencies(type, list, dep_step)
    end

    last_deps.each do |step|
      next if compute_deps.include? step
      execute_and_dup(step, dep_step)
    end

    compute_last_deps.each do |type,list|
      run_compute_dependencies(type, list, dep_step)
    end

  end

  def stop_dependencies
    dependencies.each do |dep|
      dep.abort
    end
    kill_children
  end

  #def run_dependencies
  #  @seen ||= []
  #  seen_paths ||= Set.new
  #  
  #  consolidate_dependencies
  #  dependencies.uniq.each do |dependency| 
  #    dependency_path = dependency.path
  #    next if seen_paths.include? dependency_path
  #    @seen.concat dependency.rec_dependencies
  #    seen_paths.union(dependency.rec_dependencies.collect{|d| d.path})
  #    @seen << dependency
  #    seen_paths << dependency_path
  #  end

  #  @seen.uniq!
  #  @seen.delete self

  #  return if @seen.empty?

  #  log :dependencies, "#{Log.color :magenta, "Dependencies"} for step #{Log.color :yellow, task.name.to_s || ""}"

  #  @seen.each do |dependency| 
  #    Step.prepare_for_execution(dependency)
  #  end

  #  pre_deps = []
  #  compute_pre_deps = {}
  #  last_deps = []
  #  compute_last_deps = {}
  #  @seen.each do |dependency| 
  #    if dependencies.include?(dependency) and dependency.inputs.flatten.select{|i| Step === i}.any?
  #      if ComputeDependency === dependency
  #        compute_last_deps[dependency.compute] ||= []
  #        compute_last_deps[dependency.compute] << dependency
  #      else
  #        last_deps << dependency
  #      end
  #    else
  #      if ComputeDependency === dependency
  #        compute_pre_deps[dependency.compute] ||= []
  #        compute_pre_deps[dependency.compute] << dependency
  #      else
  #        pre_deps << dependency if dependencies.include?(dependency)
  #      end
  #    end
  #  end

  #  pre_deps.each do |dependency|
  #    dependency.dup_inputs
  #    execute_dependency(dependency)
  #  end

  #  compute_pre_deps.each do |type,list|
  #    if Array === type
  #      type, *rest = type
  #    end

  #    case type
  #    when :bootstrap
  #      cpus = rest.nil? ? nil : rest.first 
  #      cpus = 10 if cpus.nil?

  #      list.each do |dependency|
  #        dependency.dup_inputs
  #      end

  #      Misc.bootstrap(list, cpus, :bar => "Bootstrapping dependencies for #{path}", :_respawn => :always) do |dep|
  #        dep.produce
  #        nil
  #      end
  #    else
  #      list.each do |dependency|
  #        dependency.dup_inputs
  #        execute_dependency(dependency)
  #      end
  #    end
  #  end

  #  last_deps.each do |dependency|
  #    dependency.dup_inputs
  #  end

  #  last_deps.each do |dependency|
  #    execute_dependency(dependency)
  #  end

  #  compute_last_deps.each do |type,list|
  #    case type
  #    when :_bootstrap
  #      list.each do |dependency|
  #        dependency.dup_inputs
  #      end
  #      Misc.bootstrap(list, 3, :bar => "Boostrapping dependencies for #{path}", :respawn => :always) do |dependency|
  #        dependency.produce
  #        nil
  #      end
  #    else
  #      list.each do |dependency|
  #        dependency.dup_inputs
  #        execute_dependency(dependency)
  #      end
  #    end
  #  end
  #end
end
