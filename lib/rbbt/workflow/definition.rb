require 'rbbt-util'
require 'rbbt/workflow/annotate'

module Workflow
  include AnnotatedModule

  AnnotatedModule.add_consummable_annotation(self,
                                             :result_description => "",
                                             :result_type        => nil,
                                             :extension          => '',
                                             :dependencies       => [])
  def helper(name, &block)
    helpers[name] = block
  end

  def desc(description)
    @description = description
  end

  def extension(extension)
    @extension = extension
  end

  def returns(description)
    @result_description = description
  end

  def dep(*dependency_list, &block)
    dependency_list << block if block_given?
    dependencies.concat dependency_list
  end

  def task(name, &block)
    if Hash === name
      type = name.first.last
      name = name.first.first
    else
      result_type = consume_result_type || :marshal
    end

    name = name.to_sym

    block = self.method(name) unless block_given?

    task_info = {
      :name => name,
      :inputs             => consume_inputs,
      :description        => consume_description,
      :input_types        => consume_input_types,
      :result_type        => (Array === type ? type.to_sym : type),
      :input_defaults     => consume_input_defaults,
      :input_descriptions => consume_input_descriptions,
      :extension          => consume_extension,
      :input_options      => consume_input_options
    }
    
    task = Task.setup(task_info, &block)

    last_task = task

    tasks[name] = task
    task_dependencies[name] = consume_dependencies
  end
  
  def export_exec(*names)
    exec_exports.concat names
  end

  def export_asynchronous(*names)
    asynchronous_exports.concat names
  end

  def export_synchronous(*names)
    synchronous_exports.concat names
  end
end
