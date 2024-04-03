module Workflow
  def rec_inputs(task_name)
    tasks[task_name].recursive_inputs.collect{|name, _| name }
  end

  def rec_input_types(task_name)
    tasks[task_name].recursive_inputs.collect{|name, type, _| type }
  end

  def rec_input_descriptions(task_name)
    tasks[task_name].recursive_inputs.collect{|name, type, desc, _| desc }
  end

  def rec_input_defaults(task_name)
    tasks[task_name].recursive_inputs.collect{|name, type, desc, default| default }
  end

  def rec_input_options(task_name)
    tasks[task_name].recursive_inputs.collect{|name, type, desc, default, options| options }
  end


  def rec_input_use(task_name)
    input_use = {}
    task = self.tasks[task_name]
    task.inputs.each do |name,_| 
      input_use[name] ||= {} 
      input_use[name][self] ||= []
      input_use[name][self] << task_name
    end

    task.deps.inject(input_use) do |acc,p|
      workflow, task_name = p
      workflow.rec_input_use(task_name).each do |name,uses|
        acc[name] ||= {}
        uses.each do |workflow, task_names|
          acc[name][workflow] ||= []
          acc[name][workflow].concat(task_names)
        end
      end
      acc
    end if task.deps

    input_use
  end
end
