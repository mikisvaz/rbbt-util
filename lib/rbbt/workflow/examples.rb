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
    tasks[task_name].input_types.collect do |input,type|
      next unless example_dir[task_name][example][input].exists?
      [input, type, example_dir[task_name][example][input].find]
    end.compact
  end
end
