require 'rbbt/util/misc'
require 'rbbt/persist'

module Task
  attr_accessor :inputs, :input_types, :result_type, :input_defaults, :input_descriptions, :input_options, :description, :name, :result_description, :extension

  def self.setup(options = {}, &block)
    block.extend Task
    options = IndiferentHash.setup options
    block.singleton_methods.
      select{|method| method.to_s[-1] != "="[0]}.each{|method|
      if block.respond_to?(method.to_s + "=") and options.include? method.to_sym
        block.send(method.to_s + '=', options[method.to_sym]) 
      end
    }
    block
  end

  def parse_description
    if description =~ /\n\n/
      short_description, rest = description.match(/(.*?)\n\n(.*)/).values_at 1, 2
    else
      short_description = description
      rest = nil
    end

    if rest.nil?
      long_description = ""
    end
  end

  def take_input_values(input_values)
    return [] if @inputs.nil?
    values = []
    defaults = IndiferentHash.setup(@input_defaults || {})
    @inputs.each do |input|
      value = input_values[input]
      value = defaults[input] if value.nil?
      values << value
    end
    values
  end

  def exec(*args)
    case
    when (args.length == 1 and not inputs.nil? and inputs.length > 1 and Hash === args.first)
      self.call *take_input_values(IndiferentHash.setup(args.first))
    else
      self.call *args
    end
  end

  def exec_in(object, *args)
    case
    when (args.length == 1 and not inputs.nil? and inputs.length > 1 and Hash === args.first)
      object.instance_exec *IndiferentHash.setup(args.first).values_at(*inputs), &self
    else
      object.instance_exec *args, &self 
    end
  end

  def persist_exec(filename, *args)
    Persist.persist "Task", @persistence_type, :file => filename do
      exec *args
    end
  end

  def persist_exec_in(filename, *args)
    Persist.persist "Task", @persistence_type, :file => filename do
      exec_in *args
    end
  end

  def self.dep_inputs(deps, workflow = nil)
    seen = []
    task_inputs = {}
    deps.each do |dep|
      wf, task = (Array === dep ? [dep.first, dep.first.tasks[dep[1].to_sym]] : [workflow, workflow.tasks[dep.to_sym]])
      maps = (Array === dep and Hash === dep.last) ? dep.last.keys : []
      raise "Dependency task not found: #{dep}" if task.nil?
      next if seen.include? [wf, task.name]
      seen << [wf, task.name]
      new_inputs = task.inputs - maps
      next unless new_inputs.any?
      task_inputs[task] = new_inputs
    end
    task_inputs
  end

  def dep_inputs(deps, workflow = nil)
    task_inputs = Task.dep_inputs deps, workflow
    task_inputs.each do |task, inputs|
      inputs.replace (inputs - self.inputs)
    end
  end
end
