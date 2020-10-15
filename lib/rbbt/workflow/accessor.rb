require 'rbbt/util/open' 
require 'yaml'

module ComputeDependency
  attr_accessor :compute
  def self.setup(dep, value)
    dep.extend ComputeDependency
    dep.compute = value
  end
  
  def canfail?
    compute == :canfail || (Array === compute && compute.include?(:canfail))
  end
end


module Workflow

  def log(status, message = nil, &block)
    Step.log(status, message, nil, &block)
  end

  def task_info(name)
    name = name.to_sym
    task = tasks[name]
    raise "No '#{name}' task in '#{self.to_s}' Workflow" if task.nil?
    id = File.join(self.to_s, name.to_s)
    @task_info ||= {}
    @task_info[id] ||= begin 
                         description = task.description
                         result_description = task.result_description
                         result_type = task.result_type
                         inputs = rec_inputs(name).uniq
                         input_types = rec_input_types(name)
                         input_descriptions = rec_input_descriptions(name)
                         input_use = rec_input_use(name)
                         input_defaults = rec_input_defaults(name)
                         input_options = rec_input_options(name)
                         extension = task.extension
                         export = case
                                  when (synchronous_exports.include?(name.to_sym) or synchronous_exports.include?(name.to_s))
                                    :synchronous
                                  when (asynchronous_exports.include?(name.to_sym) or asynchronous_exports.include?(name.to_s))
                                    :asynchronous
                                  when (exec_exports.include?(name.to_sym) or exec_exports.include?(name.to_s))
                                    :exec
                                  when (stream_exports.include?(name.to_sym) or stream_exports.include?(name.to_s))
                                    :stream
                                  else
                                    :none
                                  end

                         dependencies = task_dependencies[name].select{|dep| String === dep or Symbol === dep}
                         { :id => id,
                           :description => description,
                           :export => export,
                           :inputs => inputs,
                           :input_types => input_types,
                           :input_descriptions => input_descriptions,
                           :input_defaults => input_defaults,
                           :input_options => input_options,
                           :input_use => input_use,
                           :result_type => result_type,
                           :result_description => result_description,
                           :dependencies => dependencies,
                           :extension => extension
                         }
                       end
  end

  def rec_dependencies(taskname)
    @rec_dependencies ||= {}
    @rec_dependencies[taskname] ||= begin
                                      if task_dependencies.include? taskname

                                        deps = task_dependencies[taskname]

                                        #all_deps = deps.select{|dep| String === dep or Symbol === dep or Array === dep}

                                        all_deps = []
                                        deps.each do |dep| 
                                          if DependencyBlock === dep
                                            all_deps << dep.dependency if dep.dependency
                                          else
                                            all_deps << dep unless Proc === dep
                                          end

                                          begin
                                            case dep
                                            when Array
                                              wf, t, o = dep

                                              wf.rec_dependencies(t.to_sym).each do |d|
                                                if Array === d
                                                  new = d.dup
                                                else
                                                  new = [dep.first, d]
                                                end

                                                if Hash === o and not o.empty? 
                                                  if Hash === new.last
                                                    hash = new.last.dup
                                                    o.each{|k,v| hash[k] ||= v}
                                                    new[new.length-1] = hash
                                                  else
                                                    new.push o.dup
                                                  end
                                                end

                                                all_deps << new
                                              end if wf && t

                                            when String, Symbol
                                              rec_deps = rec_dependencies(dep.to_sym)
                                              all_deps.concat rec_deps
                                            when DependencyBlock
                                              dep = dep.dependency
                                              raise TryAgain
                                            end
                                          rescue TryAgain
                                            retry
                                          end
                                        end
                                        all_deps.uniq
                                      else
                                        []
                                      end
                                    end
  end

  def task_from_dep(dep)
    task = case dep
           when Array
             dep.first.tasks[dep[1]] 
           when String
             tasks[dep.to_sym]
           when Symbol
             tasks[dep.to_sym]
           end
    raise "Unknown dependency: #{Misc.fingerprint dep}" if task.nil?
    task
  end

  #def rec_inputs(taskname)
  #  [taskname].concat(rec_dependencies(taskname)).inject([]){|acc, tn| acc.concat(task_from_dep(tn).inputs) }.uniq
  #end

  def rec_inputs(taskname)
    task = task_from_dep(taskname)
    deps = rec_dependencies(taskname)
    dep_inputs = task.dep_inputs deps, self
    task.inputs + dep_inputs.values.flatten
  end

  def rec_input_defaults(taskname)
    rec_inputs = rec_inputs(taskname)
    [taskname].concat(rec_dependencies(taskname)).inject(IndiferentHash.setup({})){|acc, tn|
      if Array === tn and tn[0] and tn[1]
        new = tn.first.tasks[tn[1].to_sym].input_defaults
      elsif Symbol === tn
        new = tasks[tn.to_sym].input_defaults
      else
        next acc
      end
      acc = new.merge(acc) 
      acc.delete_if{|input,defaults| not rec_inputs.include? input}
      acc
    }.tap{|h| IndiferentHash.setup(h)}
  end

  def rec_input_types(taskname)
    rec_inputs = rec_inputs(taskname)
    [taskname].concat(rec_dependencies(taskname)).inject({}){|acc, tn|
      if Array === tn and tn[0] and tn[1]
        new = tn.first.tasks[tn[1].to_sym].input_types
      elsif Symbol === tn
        new = tasks[tn.to_sym].input_types
      else
        next acc
      end
      acc = new.merge(acc) 
      acc.delete_if{|input,defaults| not rec_inputs.include? input}
      acc
    }.tap{|h| IndiferentHash.setup(h)}
  end

  def rec_input_use(taskname)
    task = task_from_dep(taskname)
    deps = rec_dependencies(taskname)
    inputs = {}
    task.inputs.each do |input|
      name = task.name
      workflow = (task.workflow || self).to_s

      inputs[input] ||= {}
      inputs[input][workflow] ||= []
      inputs[input][workflow]  << name
    end

    dep_inputs = Task.dep_inputs deps, self

    dep_inputs.each do |dep,is|
      name = dep.name
      workflow = dep.workflow

      is.each do |input|
        inputs[input] ||= {}
        inputs[input][workflow] ||= []
        inputs[input][workflow]  << name
      end
    end

    inputs
  end

  def rec_input_descriptions(taskname)
    rec_inputs = rec_inputs(taskname)
    [taskname].concat(rec_dependencies(taskname)).inject({}){|acc, tn|
      if Array === tn and tn[0] and tn[1]
        new = tn.first.tasks[tn[1].to_sym].input_descriptions
      elsif Symbol === tn
        new = tasks[tn.to_sym].input_descriptions
      else
        next acc
      end
      acc = new.merge(acc) 
      acc.delete_if{|input,defaults| not rec_inputs.include? input}
      acc
    }.tap{|h| IndiferentHash.setup(h)}
  end

  def rec_input_options(taskname)
    rec_inputs = rec_inputs(taskname)
    [taskname].concat(rec_dependencies(taskname)).inject({}){|acc, tn|
      if Array === tn and tn[0] and tn[1]
        new = tn.first.tasks[tn[1].to_sym].input_options
      elsif Symbol === tn
        new = tasks[tn.to_sym].input_options
      else
        next acc
      end
      acc = new.merge(acc) 
      acc = acc.delete_if{|input,defaults| not rec_inputs.include? input}
      acc
    }.tap{|h| IndiferentHash.setup(h)}
  end

  def assign_dep_inputs(_inputs, options, all_d, task_info)
    options.each{|i,v|
      next if i == :compute or i == "compute"
      case v
      when :compute
        compute = v
      when Symbol
        rec_dependency = all_d.flatten.select{|d| d.task_name.to_sym == v }.first

        if rec_dependency.nil?
          if _inputs.include? v
            _inputs[i] = _inputs.delete(v)
          else
            _inputs[i] = v unless _inputs.include? i
          end
        else
          input_options = task_info[:input_options][i] || {}
          if input_options[:stream] or true
            #rec_dependency.run(true).grace unless rec_dependency.done? or rec_dependency.running?
            _inputs[i] = rec_dependency
          else
            rec_dependency.abort if rec_dependency.streaming? and not rec_dependency.running?
            rec_dependency.clean if rec_dependency.error? or rec_dependency.aborted?
            if rec_dependency.streaming? and rec_dependency.running?
              _inputs[i] = rec_dependency.join.load
            else
              rec_dependency.run(true)
              rec_dependency.join
              _inputs[i] = rec_dependency.load
            end
          end
        end
      else
        _inputs[i] = v
      end
    } if options

    _inputs
  end

  def override_dependencies(inputs)
    override_dependencies = IndiferentHash.setup({})
    return override_dependencies if inputs.nil?
    inputs.each do |key,value|
      if String === key && m = key.match(/(.*)#(.*)/)
        workflow, task = m.values_at 1, 2
        workflow = self.to_s if workflow.empty?
        override_dependencies[workflow] ||= IndiferentHash.setup({})
        override_dependencies[workflow][task] = value
      end
    end
    override_dependencies
  end

  def setup_override_dependency(dep, workflow, task_name)
    dep = Step === dep ? dep : Workflow.load_step(dep)
    dep.info[:name] = dep.name
    begin
      workflow = Kernel.const_get workflow if String === workflow
      dep.task = workflow.tasks[task_name] if dep.task.nil? && workflow.tasks.include?(task_name)
    rescue
      Log.exception $!
    end
    dep.task_name = task_name
    dep.overriden = true
    dep
  end

  def real_dependencies(task, orig_jobname, inputs, dependencies)
    real_dependencies = []
    path_deps = {}

    override_dependencies = override_dependencies(inputs)

    dependencies.each do |dependency|
      _inputs = IndiferentHash.setup(inputs.dup)
      jobname = orig_jobname
      jobname = _inputs[:jobname] if _inputs.include? :jobname

      real_dep = case dependency
                 when Array
                   workflow, dep_task, options = dependency

                   if override_dependencies[workflow.to_s] && value = override_dependencies[workflow.to_s][dep_task]
                     setup_override_dependency(value, workflow, dep_task)
                   else

                     compute = options[:compute] if options

                     all_d = (real_dependencies + real_dependencies.flatten.collect{|d| d.rec_dependencies} ).flatten.compact.uniq

                     _inputs = assign_dep_inputs(_inputs, options, all_d, workflow.task_info(dep_task))
                     jobname = _inputs.delete :jobname if _inputs.include? :jobname

                     job = workflow._job(dep_task, jobname, _inputs)
                     ComputeDependency.setup(job, compute) if compute
                     job
                   end
                 when Step
                   dependency
                 when Symbol
                   if override_dependencies[self.to_s] && value = override_dependencies[self.to_s][dependency]
                     setup_override_dependency(value, self, dependency)
                   else
                     _job(dependency, jobname, _inputs)
                   end
                 when Proc
                   if DependencyBlock === dependency
                     orig_dep = dependency.dependency 
                     wf, task_name, options = orig_dep

                     options = {} if options.nil?
                     compute = options[:compute]

                     options = IndiferentHash.setup(options.dup)
                     dep = dependency.call jobname, options.merge(_inputs), real_dependencies

                     dep = [dep] unless Array === dep

                     new_=[]
                     dep.each{|d| 
                       next if d.nil?
                       if Hash === d
                         d[:workflow] ||= wf 
                         d[:task] ||= task_name
                         _override_dependencies = override_dependencies.merge(override_dependencies(d[:inputs] || {}))
                         d = if _override_dependencies[d[:workflow].to_s] && value = _override_dependencies[d[:workflow].to_s][d[:task]]
                               setup_override_dependency(value, d[:workflow], d[:task])
                             else
                               task_info = d[:workflow].task_info(d[:task])

                               inputs = assign_dep_inputs({}, options.merge(d[:inputs] || {}), real_dependencies, task_info) 
                               d[:workflow]._job(d[:task], d[:jobname], inputs) 
                             end
                       end
                       ComputeDependency.setup(d, compute) if compute
                       new_ << d
                     }
                     dep = new_
                   else
                     _inputs = IndiferentHash.setup(_inputs.dup)
                     dep = dependency.call jobname, _inputs, real_dependencies
                     if Hash === dep
                       dep[:workflow] ||= wf || self
                       _override_dependencies = override_dependencies.merge(override_dependencies(dep[:inputs] || {}))
                       if _override_dependencies[dep[:workflow].to_s] && value = _override_dependencies[dep[:workflow].to_s][dep[:task]]
                         setup_override_dependency(value, dep[:workflow], dep[:task])
                       else
                         task_info = (dep[:task] && dep[:workflow]) ? dep[:workflow].task_info(dep[:task]) : nil
                         inputs = assign_dep_inputs({}, dep[:inputs], real_dependencies, task_info)
                         dep = dep[:workflow]._job(dep[:task], dep[:jobname], inputs)
                       end
                     end
                   end

                   dep
                 else
                   raise "Dependency for #{task.name} not understood: #{Misc.fingerprint dependency}"
                 end

      real_dependencies << real_dep
    end
    real_dependencies.flatten.compact
  end

  TAG = ENV["RBBT_INPUT_JOBNAME"] == "true" ? :inputs : :hash
  DEBUG_JOB_HASH = ENV["RBBT_DEBUG_JOB_HASH"] == 'true'
  def step_path(taskname, jobname, inputs, dependencies, extension = nil)
    raise "Jobname makes an invalid path: #{ jobname }" if jobname.include? '..'
    if inputs.length > 0 or dependencies.any?
      tagged_jobname = case TAG
                       when :hash
                         clean_inputs = Annotated.purge(inputs)
                         clean_inputs = clean_inputs.collect{|i| Symbol === i ? i.to_s : i }
                         deps_str = dependencies.collect{|d| (Step === d || (defined?(RemoteStep) && RemoteStep === Step)) ? "Step: " << d.short_path : d }
                         key_obj = {:inputs => clean_inputs, :dependencies => deps_str }
                         key_str = Misc.obj2str(key_obj)
                         hash_str = Misc.digest(key_str)
                         Log.debug "Hash for '#{[taskname, jobname] * "/"}' #{hash_str} for #{key_str}" if DEBUG_JOB_HASH
                         jobname + '_' << hash_str
                       when :inputs
                         all_inputs = {}
                         inputs.zip(self.task_info(taskname)[:inputs]) do |i,f|
                           all_inputs[f] = i
                         end
                         dependencies.each do |dep|
                           ri = dep.recursive_inputs
                           ri.zip(ri.fields).each do |i,f|
                             all_inputs[f] = i
                           end
                         end

                         all_inputs.any? ? jobname + '_' << Misc.obj2str(all_inputs) : jobname
                       else
                         jobname
                       end
    else
      tagged_jobname = jobname
    end

    if extension and not extension.empty?
      tagged_jobname = tagged_jobname + ('.' << extension.to_s)
    end

    workdir[taskname][tagged_jobname].find
  end

  def id_for(path)
    if workdir.respond_to? :find
      workdir_find = workdir.find 
    else
      workdir_find = workdir
    end
    Misc.path_relative_to workdir_find, path
  end

  def self.workflow_for(path)
    begin
      Kernel.const_get File.dirname(File.dirname(path))
    rescue
      nil
    end
  end

  def task_for(path)
    if workdir.respond_to? :find
      workdir_find = workdir.find 
    else
      workdir_find = workdir
    end

    workdir_find = File.expand_path(workdir_find)
    path = File.expand_path(path)
    dir = File.dirname(path)
    begin
      Misc.path_relative_to(workdir_find, dir).sub(/([^\/]+)\/.*/,'\1')
    rescue
      nil
    end
  end

  def task_exports
    [exec_exports, synchronous_exports, asynchronous_exports, stream_exports].compact.flatten.uniq
  end
end
