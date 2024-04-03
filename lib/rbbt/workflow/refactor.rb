require_relative 'refactor/export'
require_relative 'refactor/recursive'
require_relative 'refactor/task_info'

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

  def self.save_inputs(inputs, input_types, dir)
    inputs.each do |name,value|
      next if value.nil?
      type = input_types[name]
      type = type.to_s if type

      Task.save_input(dir, name, type, value)
    end.any?
  end

  def soft_grace
    sleep 1 until ! Open.exists?(info_file)
  end
end

