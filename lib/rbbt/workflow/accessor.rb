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

  def self.job_path?(path)
    path.split("/")[-4] == "jobs"
  end

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
