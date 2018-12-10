require 'rbbt-util'
require 'rbbt/util/misc/annotated_module'

module Workflow

  module DependencyBlock
    attr_accessor :dependency
    def self.setup(block, dependency)
      block.extend DependencyBlock
      block.dependency = dependency
      block
    end
  end

  include InputModule
  AnnotatedModule.add_consummable_annotation(self,
    :dependencies       => [],
    :description        => "",
    :result_type        => nil,
    :result_description => "",
    :extension          => '')


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

  def dep(*dependency, &block)
    @dependencies ||= []
    if block_given?
      if dependency.any?

        wf, task_name, options = dependency
        options, task_name = task_name, nil if Hash === task_name
        options, wf = wf, nil if Hash === wf
        task_name, wf = wf, self if task_name.nil?

        DependencyBlock.setup block, [wf, task_name, options] 
      end
      @dependencies << block
    else
      if Module === dependency.first or 
        (defined? WorkflowRESTClient and WorkflowRESTClient === dependency.first) or
        Hash === dependency.last

        dependency = ([self] + dependency) unless Module === dependency.first or (defined? WorkflowRESTClient and WorkflowRESTClient === dependency.first)
        @dependencies << dependency
      else
        @dependencies.concat dependency
      end
    end
  end

  FORGET_DEP_TASKS = ENV["RBBT_FORGET_DEP_TASKS"] == "true"
  def dep_task(name, *dependency, &block)
    dep(*dependency, &block)
    task name do
      dep = dependencies.last.join
      set_info :result_type, dep.info[:result_type]
      forget = config :forget_dep_tasks, :forget_dep_tasks, :default => FORGET_DEP_TASKS
      if forget
        Open.ln_h dep.path, self.path
        self.dependencies = self.dependencies - [dep]
        self.set_info :dependency, dependencies.collect{|dep| [dep.task_name, dep.name, dep.path]}
      else
        Open.link dep.path, self.path
      end
      nil
    end
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
      :name               => name,
      :inputs             => consume_inputs,
      :description        => consume_description,
      :input_types        => consume_input_types,
      :result_type        => (String === type ? type.to_sym : type),
      :result_description => consume_result_description,
      :input_defaults     => consume_input_defaults,
      :input_descriptions => consume_input_descriptions,
      :required_inputs    => consume_required_inputs,
      :extension          => consume_extension,
      :input_options      => consume_input_options
    }
    
    task = Task.setup(task_info, &block)

    last_task = task

    tasks[name] = task
    task_dependencies[name] = consume_dependencies
  end

  def unexport(*names)
    names = names.collect{|n| n.to_s} + names.collect{|n| n.to_sym}
    names.uniq!
    exec_exports.replace exec_exports - names if exec_exports
    synchronous_exports.replace synchronous_exports - names if synchronous_exports
    asynchronous_exports.replace asynchronous_exports - names if asynchronous_exports
    stream_exports.replace stream_exports - names if stream_exports
  end
  
  def export_exec(*names)
    unexport *names
    exec_exports.concat names
    exec_exports.uniq!
    exec_exports
  end

  def export_synchronous(*names)
    unexport *names
    synchronous_exports.concat names
    synchronous_exports.uniq!
    synchronous_exports
  end

  def export_asynchronous(*names)
    unexport *names
    asynchronous_exports.concat names
    asynchronous_exports.uniq!
    asynchronous_exports
  end

  def export_stream(*names)
    unexport *names
    stream_exports.concat names
    stream_exports.uniq!
    stream_exports
  end

  alias export export_asynchronous
end
