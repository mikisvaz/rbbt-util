module Workflow
  attr_accessor :example_dir

  def example_dir
    @example_dir ||= self.libdir.examples
  end

  def examples
    return {} unless self.libdir.examples.exists?
    examples = {}
    example_dir.glob("*/*").each do |example_dir|
      example = File.basename(example_dir)
      task_name = File.basename(File.dirname(example_dir))
      examples[task_name] ||= []
      examples[task_name] << example
    end
    IndiferentHash.setup examples
    examples
  end

  def example(task_name, example)
    task_info(task_name.to_sym)[:input_types].collect do |input,type|
      next unless example_dir[task_name][example][input].exists?
      [input, type, example_dir[task_name][example][input].find]
    end.compact
  end

  #def example_inputs_old(task_name, example)
  #  inputs = {}
  #  IndiferentHash.setup(inputs)
  #  example(task_name, example).each do |input,type,file|

  #    case type
  #    when :tsv, :array, :text
  #      Log.debug "Pointing #{ input } to #{file}"
  #      inputs[input.to_sym]  = file
  #    when :boolean
  #      inputs[input.to_sym]  = (file.read.strip == 'true')
  #    else
  #      Log.debug "Loading #{ input } from #{file}"
  #      inputs[input.to_sym]  = file.read.strip
  #    end
  #  end
  #  inputs
  #end

  def example_inputs(task_name, example)
    dir = example_dir[task_name][example]
    info = self.task_info(task_name)
    Workflow.load_inputs(dir, info[:inputs], info[:input_types])
  end

  def example_step(task_name, example="Example", new_inputs = {})
    inputs = example_inputs(task_name, example)

    if new_inputs and new_inputs.any?
      IndiferentHash.setup(new_inputs)
      inputs = inputs.merge(new_inputs)
    end

    self.job(task_name, example, inputs)
  end
end
