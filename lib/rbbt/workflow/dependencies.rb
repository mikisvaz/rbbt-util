module Workflow
  def rec_dependencies(taskname, seen = [])
    @rec_dependencies ||= {}
    @rec_dependencies[taskname] ||= [] unless task_dependencies.include?(taskname)
    @rec_dependencies[taskname] ||= begin

                                      deps = task_dependencies[taskname]

                                      all_deps = []
                                      deps.each do |dep| 
                                        next if seen.include?(dep)
                                        if DependencyBlock === dep
                                          all_deps << dep.dependency if dep.dependency
                                        else
                                          all_deps << dep unless Proc === dep
                                        end

                                        begin
                                          case dep
                                          when Array
                                            wf, t, o = dep

                                            wf.rec_dependencies(t.to_sym, seen + [dep]).each do |d|
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
                                            rec_deps = rec_dependencies(dep.to_sym, seen + [dep])
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
                                    end
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
    return [] if dep == :skip || dep == 'skip'
    dep = Step === dep ? dep : Workflow.load_step(dep)
    dep.workflow = workflow
    dep.info[:name] = dep.name
    dep.original_task_name ||= dep.task_name if dep.workflow


    begin
      workflow = Kernel.const_get workflow if String === workflow
      dep.task = workflow.tasks[task_name] if dep.task.nil? && workflow.tasks.include?(task_name)
    rescue
      Log.exception $!
    end
    dep.task_name = task_name
    dep.overriden = dep.original_task_name.to_sym

    dep.extend step_module

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

                     if override_dependencies[wf.to_s] && value = override_dependencies[wf.to_s][task_name]
                       dep = setup_override_dependency(value, wf, task_name)
                     else

                       options = {} if options.nil?
                       compute = options[:compute]

                       options = IndiferentHash.setup(options.dup)
                       dep = dependency.call jobname, _inputs.merge(options), real_dependencies

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

                                 _inputs = assign_dep_inputs({}, options.merge(d[:inputs] || {}), real_dependencies, task_info) 
                                 d[:workflow]._job(d[:task], d[:jobname], _inputs) 
                               end
                         end
                         ComputeDependency.setup(d, compute) if compute
                         new_ << d
                       }
                       dep = new_
                     end
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
                         _inputs = assign_dep_inputs({}, dep[:inputs], real_dependencies, task_info)
                         dep = dep[:workflow]._job(dep[:task], dep[:jobname], _inputs)
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
end