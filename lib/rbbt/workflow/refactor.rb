require_relative 'refactor/export'
require_relative 'refactor/recursive'
require_relative 'refactor/task_info'
require_relative 'refactor/inputs'

class Step
  alias get_stream stream

  def self.md5_file(path)
    path.nil? ? nil : path + '.md5'
  end

  def md5_file
    Step.md5_file(path)
  end

  alias real_inputs non_default_inputs
end

module Workflow
  alias workdir= directory=
  
  def resumable
    Log.warn "RESUMABLE MOCKED"
  end

  DEFAULT_NAME = Task::DEFAULT_NAME

end

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

class Step

  def soft_grace
    sleep 1 until Open.exists?(info_file)
  end
end

Rbbt.relay_module_method Workflow, :load_step, Step, :load
Rbbt.relay_module_method Workflow, :fast_load_step, Step, :load

module Workflow
  attr_accessor :remote_tasks
  def remote_tasks
    @remote_tasks ||= {}
  end
  def task_for(path)
    parts = path.split("/")
    if parts.include?(self.to_s)
      parts[parts.index(self.to_s) + 1]
    else
      parts[-2]
    end
  end

  def fast_load_id(id)
    path = if Path === directory
             directory[id].find
           else
             File.join(directory, id)
           end
    task = task_for path
    return remote_tasks[task].load_id(id) if remote_tasks && remote_tasks.include?(task)
    return Workflow.fast_load_step path
  end

  alias load_id fast_load_id
end
