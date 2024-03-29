class Step

  STREAM_CACHE = {}
  STREAM_CACHE_MUTEX = Mutex.new
  def self.purge_stream_cache
    # Log.debug "Purging dup. stream cache"
    STREAM_CACHE_MUTEX.synchronize do
      STREAM_CACHE.clear
    end
  end

  def self.dup_stream(stream)
    case stream
    when IO, File, Step
      return stream if stream.respond_to?(:closed?) and stream.closed?
      return stream if stream.respond_to?(:done?) and stream.done?

      STREAM_CACHE_MUTEX.synchronize do
        stream_key = Misc.fingerprint(stream)
        current = STREAM_CACHE[stream_key]
        case current
        when nil, Step
          Log.medium "Not duplicating stream #{stream_key}"
          STREAM_CACHE[stream_key] = stream
        when File
          if Open.exists?(current.path)
            Log.medium "Reopening file #{stream_key}"
            Open.open(current.path)
          else
            new = Misc.dup_stream(current)
            Log.medium "Duplicating file #{stream_key} #{current.inspect} => #{Misc.fingerprint(new)}"
            new
          end
        else
          new = Misc.dup_stream(current)
          Log.medium "Duplicating stream #{stream_key} #{ Misc.fingerprint(stream) } => #{Misc.fingerprint(new)}"
          new
        end
      end
    when TSV::Dumper, TSV::Parser
      orig_stream = stream
      stream = stream.stream
      return stream if stream.closed?

      STREAM_CACHE_MUTEX.synchronize do
        if STREAM_CACHE[stream].nil?
          Log.high "Not duplicating #{Misc.fingerprint orig_stream} #{ stream.inspect }"
          STREAM_CACHE[stream] = stream
        else
          new = Misc.dup_stream(STREAM_CACHE[stream])
          Log.high "Duplicating #{Misc.fingerprint orig_stream} #{ stream.inspect } into #{new.inspect}"
          new
        end
      end
    else
      stream
    end
  end

  def dup_inputs
    return if @inputs.nil?
    return if @dupped or ENV["RBBT_NO_STREAM"] == 'true'
    return if ComputeDependency === self and self.compute == :produce
    Log.low "Dupping inputs for #{path}"
    dupped_inputs = @inputs.collect do |input|
      Step.dup_stream input
    end
    @inputs.replace dupped_inputs
    @dupped = true
  end

  def self.prepare_for_execution(job)
    return if job.done? && ! job.dirty?

    status = job.status.to_s

    if defined?(WorkflowRemoteClient) && WorkflowRemoteClient::RemoteStep === job 
      return unless (status == 'done' or status == 'error' or status == 'aborted')
    else
      return if status == 'streaming' and job.running?
    end

    canfail = nil
    job.status_lock.synchronize do
      status = job.status.to_s

      if (status == 'error' && (job.recoverable_error? || job.dirty?)) ||
          (job.noinfo? && Open.exists?(job.pid_file)) ||
          job.aborted? ||
          (job.done? && ! job.updated?)  || (job.error? && ! job.updated?) ||
          (job.done? && job.dirty?)  || (job.error? && job.dirty?) ||
          (!(job.noinfo? || job.done? || job.error? || job.aborted? || job.running?))

        if ! (job.resumable? && (job.updated? && ! job.dirty?))
          Log.high "About to clean -- status: #{status}, present #{File.exist?(job.path)}, " +
            %w(done? error? recoverable_error? noinfo? updated? dirty? aborted? running? resumable?).
            collect{|v| [v, job.send(v)]*": "} * ", " if RBBT_DEBUG_CLEAN

          job.clean
        end
        job.set_info :status, :cleaned
      end

      job.dup_inputs unless status == 'done' or job.started?
      job.init_info(status == 'noinfo') unless status == 'waiting' || status == 'done' || job.started? || ! Workflow.job_path?(job.path)

      canfail = ComputeDependency === job && job.canfail?
    end

    Step.raise_dependency_error(job) if job.error? and not canfail
  end

  def self.raise_dependency_error(job)
    begin
      if job.get_exception
        klass = job.get_exception.class
      else
        klass = Kernel.const_get(info[:exception][:class])
      end
    rescue
      Log.exception $!
      raise DependencyError, job 
    end

    if (klass <= RbbtException)
      raise DependencyRbbtException, job 
    else
      raise DependencyError, job 
    end
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

  def input_dependencies
    @input_dependencies ||= recursive_inputs(true).flatten.
      select{|i| Step === i || (defined?(RemoteStep) && RemoteStep === i) } + 
      recursive_inputs(true).flatten.
      select{|dep| Path === dep && Step === dep.resource }.
      #select{|dep| ! dep.resource.started? }. # Ignore input_deps already started
      collect{|dep| dep.resource }
  end

  def execute_dependency(dependency, log = true)
    task_name = self.task_name
    canfail_paths = self.canfail_paths
    already_failed = []
    begin

      dependency.resolve_input_steps

      if dependency.done?
        dependency.inputs.each do |v|
          Misc.consume_stream(v) if IO === v
          Misc.consume_stream(TSV.get_stream v) if Step === v and not v.done?  and  v.streaming?
        end
        log_dependency_exec(dependency, :done) if log
        return
      end

      dependency.status_lock.synchronize do
        if dependency.aborted? || (dependency.error? && dependency.recoverable_error? && ! canfail_paths.include?(dependency.path) && ! already_failed.include?(dependency.path)) || (!Open.remote?(dependency.path) && dependency.missing?)
          if dependency.resumable?
            dependency.status = :resume
          else
            Log.warn "Cleaning dep. on exec #{Log.color :blue, dependency.path} (missing: #{dependency.missing?}; error #{dependency.error?})"
            dependency.clean
            already_failed << dependency.path
            raise TryAgain
          end
        end
      end

      if dependency.status == :resume || ! (dependency.started? || dependency.error?)
        log_dependency_exec(dependency, :starting)
        dependency.run(true)
        raise TryAgain
      end

      dependency.grace

      if dependency.error?
        log_dependency_exec(dependency, :error)
        Step.raise_dependency_error dependency 
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
      #Log.low "Retrying dep. #{Log.color :yellow, dependency.task_name.to_s} -- [#{dependency.status}] #{(dependency.messages || ["No message"]).last}"
      retry
    rescue Aborted, Interrupt
      Log.error "Aborted dep. #{Log.color :red, dependency.task_name.to_s}"
      raise $!
    rescue Interrupt
      Log.error "Interrupted while in dep. #{Log.color :red, dependency.task_name.to_s}"
      raise $!
    rescue Exception
      Log.error "Exception in dep. #{ Log.color :red, dependency.task_name.to_s } -- #{$!.message}"
      raise $! unless canfail_paths.include? dependency.path
    end
  end

  def execute_and_dup(step, dep_step, log = true)
    dup = step.result.nil?
    execute_dependency(step, log)
    if dup and step.streaming? and not step.result.nil?
      if dep_step[step.path] and dep_step[step.path].length > 1
        stream = step.result
        other_steps = dep_step[step.path].uniq.reject{|d| d.overriden }

        other_steps = other_steps.collect{|d|
          deps_using_step_input = d.rec_dependencies.select{|d| d.inputs.include? step  }
          deps_using_step_input.any? ? deps_using_step_input : d
        }.flatten.uniq

        return unless other_steps.length > 1

        log_dependency_exec(step, "duplicating #{other_steps.length}") 
        copies = Misc.tee_stream_thread_multiple(stream, other_steps.length)
        copies.extend StreamArray
        step.instance_variable_set("@result", copies)
      end
    end
  end

  def run_compute_dependencies(type, list, dep_step = {})
    if Array === type
      type, *rest = type
    end

    canfail = (rest && rest.include?(:canfail)) || type == :canfail

    case type
    when :canfail
      list.each do |dep|
        begin
          dep.produce
        rescue RbbtException
          Log.warn "Allowing failing of #{dep.path}: #{dep.messages.last if dep.messages}"
        rescue Exception
          Log.warn "Not Allowing failing of #{dep.path} because #{$!.class} not RbbtException"
          raise $!
        end
        nil
      end
    when :produce, :no_dup
      list.each do |step|
        Misc.insist do
          begin
            step.produce
          rescue RbbtException
            raise $! unless canfail || step.canfail?
          rescue Exception
            step.exception $!
            if step.recoverable_error?
              raise $!
            else
              raise StopInsist.new($!)
            end
          end
        end
      end
      nil
    when :bootstrap
      cpus = rest.nil? ? nil : rest.first 

      if cpus.nil? 
        keys = ['bootstrap'] + list.collect{|d| [d.task_name, d.task_signature] }.flatten.uniq
        cpus = config('dep_cpus', *keys, :default => [5, list.length / 2].min)
      elsif Symbol === cpus
        cpus = config('dep_cpus', cpus, :default => [5, list.length / 2].min)
      end

      respawn = rest && rest.include?(:respawn)
      respawn = false if rest && rest.include?(:norespawn)
      respawn = rest && rest.include?(:always_respawn)
      respawn = :always if respawn.nil?

      Misc.bootstrap(list, cpus, :bar => "Bootstrapping dependencies for #{self.short_path} [#{cpus}]", :respawn => respawn) do |dep|
        begin
          Signal.trap(:INT) do
            dep.abort
            raise Aborted
          end

          Misc.insist do
            begin
              dep.produce 
              Log.warn "Error in bootstrap dependency #{dep.path}: #{dep.messages.last}" if dep.error? or dep.aborted?

            rescue Aborted
              ex = $!
              begin
                dep.abort
                Log.warn "Aborted bootstrap dependency #{dep.path}: #{dep.messages.last}" if dep.error? or dep.aborted?
              rescue
              end
              raise StopInsist.new(ex)

            rescue RbbtException
              if canfail || dep.canfail?
                Log.warn "Allowing failing of #{dep.path}: #{dep.messages.last}"
              else
                Log.warn "NOT Allowing failing of #{dep.path}: #{dep.messages.last}"
                dep.exception $!
                if dep.recoverable_error?
                  begin
                    dep.abort
                  rescue
                  end
                  raise $!
                else
                  raise StopInsist.new($!)
                end
              end
            end
          end
        rescue
          dep.abort
          raise $!
        end
        nil
      end
    else
      list.each do |step|
        execute_and_dup(step, dep_step, false)
      end
    end
  end

  def canfail_paths
    return Set.new if done? && ! Open.exists?(info_file)

    @canfail_paths ||= begin 
                         if info[:canfail] 
                           paths = info[:canfail].uniq
                           paths = Workflow.relocate_array self.path, paths if relocated
                           Set.new(paths)
                         else
                           canfail_paths = Set.new
                           all_deps = dependencies || []
                           all_deps.each do |dep|
                             next if canfail_paths.include? dep.path
                             canfail_paths += dep.canfail_paths
                             next unless ComputeDependency === dep && dep.canfail?
                             canfail_paths << dep.path
                             canfail_paths += dep.rec_dependencies.collect{|d| d.path }
                           end
                           canfail_paths
                           begin
                             set_info :canfail, canfail_paths.to_a
                           rescue Errno::EROFS
                           end
                           canfail_paths
                         end
                       end
  end

  def run_dependencies

    rec_dependencies = self.rec_dependencies(true) + input_dependencies.reject{|d| d.started? }

    return if rec_dependencies.empty?

    all_deps = rec_dependencies + [self]

    compute_deps = rec_dependencies.collect do |dep|
      next unless ComputeDependency === dep
      dep.rec_dependencies + dep.inputs.flatten.select{|i| Step === i}
    end.compact.flatten.uniq

    canfail_paths = self.canfail_paths

    dep_step = {}
    seen_paths = Set.new
    all_deps.uniq.each do |step|
      next if seen_paths.include? step.path
      seen_paths << step.path

      begin
        Step.prepare_for_execution(step) unless step == self 
      rescue DependencyError, DependencyRbbtException
        raise $! unless canfail_paths.include? step.path
      end

      next unless step.dependencies and step.dependencies.any?

      # ToDo is this really necessary
      #(step.dependencies + step.input_dependencies).each do |step_dep|
      step.dependencies.each do |step_dep|
        next unless step.dependencies.include?(step_dep)
        next if step_dep.done? or step_dep.running? or 
          (ComputeDependency === step_dep and (step_dep.compute == :nodup or step_dep.compute == :ignore))
        dep_step[step_dep.path] ||= []
        dep_step[step_dep.path] << step
      end

    end

    produced = []
    (dependencies + input_dependencies).each do |dep|
      next if dep.started?
      next unless ComputeDependency === dep
      if dep.compute == :produce
        dep.produce 
        produced << dep.path
      end
    end

    self.dup_inputs

    required_dep_paths = []
    dep_step.each do |path,list|
      required_dep_paths << path if (list & dependencies).any?
    end

    required_dep_paths.concat dependencies.collect{|dep| dep.path}

    required_dep_paths.concat input_dependencies.collect{|dep| dep.path}

    required_dep_paths.concat(dependencies.collect do |dep| 
      [dep.path] + dep.input_dependencies
    end.flatten)


    pre_deps = []
    simple_dependencies = []
    compute_simple_dependencies = {}
    compute_last_deps = {}
    seen_paths = Set.new
    rec_dependencies.uniq.reverse.each do |step| 
      next if seen_paths.include? step.path
      seen_paths << step.path
      next unless required_dep_paths.include? step.path
      required_seen_paths = seen_paths & required_dep_paths

      inputs = step.inputs
      inputs = inputs.values if Hash === inputs
      internal = inputs.select{|i| i.respond_to?(:path) && required_seen_paths.include?(i.path) }.any?

      if ComputeDependency === step 
        next if produced.include? step.path
        if internal
          compute_last_deps[step.compute] ||= []
          compute_last_deps[step.compute] << step
        else
          compute_simple_dependencies[step.compute] ||= []
          compute_simple_dependencies[step.compute] << step
        end
      else
        if internal
          simple_dependencies << step
        else
          simple_dependencies.prepend(step)
        end
      end
    end

    log :dependencies, "Processing dependencies for #{Log.color :yellow, task_name.to_s || ""}" if compute_simple_dependencies.any? || simple_dependencies.any? || compute_last_deps.any?

    Log.debug "compute_simple_dependencies: #{Misc.fingerprint(compute_simple_dependencies)} - #{Log.color :blue, self.path}" if compute_simple_dependencies.any?
    compute_simple_dependencies.each do |type,list|
      run_compute_dependencies(type, list, dep_step)
    end

    Log.low "pre_deps: #{Misc.fingerprint(pre_deps)} - #{Log.color :blue, self.path}" if pre_deps.any?
    pre_deps.each do |step|
      next if compute_deps.include? step
      begin
        execute_and_dup(step, dep_step, false)
      rescue Exception
        raise $! unless canfail_paths.include?(step.path)
      end
    end

    Log.debug "simple_dependencies: #{Misc.fingerprint(simple_dependencies)} - #{Log.color :blue, self.path}" if simple_dependencies.any?
    simple_dependencies.each do |step|
      next if compute_deps.include? step
      begin Exception
        execute_and_dup(step, dep_step) 
      rescue 
        raise $! unless canfail_paths.include?(step.path)
      end
    end

    Log.low "compute_last_deps: #{Misc.fingerprint(compute_simple_dependencies)} - #{Log.color :blue, self.path}" if compute_simple_dependencies.any?
    compute_simple_dependencies.each do |type,list|
      run_compute_dependencies(type, list, dep_step)
    end

    dangling_deps = all_deps.reject{|dep| dep.done? || canfail_paths.include?(dep.path) }.
      select{|dep| dep.waiting? }

    Log.medium "Aborting (actually not) waiting dangling dependencies #{Misc.fingerprint dangling_deps}" if dangling_deps.any?
    #dangling_deps.each{|dep| dep.abort }

  end

  def stop_dependencies
    return if dependencies.nil?
    dependencies.each do |dep|
      if dep.nil?
        Log.warn "Dependency is nil #{Misc.fingerprint step} -- #{Misc.fingerprint dependencies}"
        next
      end

      next if dep.done? or dep.aborted?

      dep.abort if dep.running?
    end
    kill_children
  end

  def overriden?
    return @overriden
    return true if @overriden
    return true if dependencies && dependencies.select{|dep| TrueClass === dep.overriden }.any?
    info[:archived_info].each do |f,i|
      next if Symbol === i
      return true if i[:overriden] || i["overriden"]
    end if info[:archived_info]
    return false
  end

  def overriden
    @overriden
    #if @overriden.nil? 
    #  return false if dependencies.nil?
    #  dependencies.select{|dep| dep.overriden? }.any?
    #else
    #  @overriden
    #end
  end

  def overriden_deps
    ord = []
    deps = dependencies.dup
    while dep = deps.shift
      case dep.overriden
      when FalseClass
        next
      when Symbol
        ord << dep
      else
        deps += dep.dependencies
      end
    end
    ord
  end

  def dependencies=(dependencies)
    @dependencies = dependencies
    set_info :dependencies, dependencies.collect{|dep| [dep.task_name, dep.name, dep.path]} if dependencies
  end

  #connected = true means that dependency searching ends when a result is done
  #but dependencies are absent, meanining that the file could have been dropped
  #in
  def rec_dependencies(connected = false, seen = [])
    # A step result with no info_file means that it was manually
    # placed. In that case, do not consider its dependencies
    return [] if ! (defined? WorkflowRemoteClient && WorkflowRemoteClient::RemoteStep === self) && ! Open.exists?(self.info_file) && Open.exists?(self.path.to_s) 

    return [] if dependencies.nil? or dependencies.empty?

    if self.overriden?
      archived_deps = []
    else
      archived_deps = self.info[:archived_info] ? self.info[:archived_info].keys : []
    end

    new_dependencies = []
    dependencies.each{|step| 
      #next if self.done? && Open.exists?(info_file) && info[:dependencies] && info[:dependencies].select{|task,name,path| path == step.path }.empty?
      next if archived_deps.include? step.path
      next if seen.include? step
      next if step.done? && connected && ! step.updatable?

      r = step.rec_dependencies(connected, new_dependencies)
      new_dependencies.concat r
      new_dependencies << step
    }

    new_dependencies.uniq
  end

end
