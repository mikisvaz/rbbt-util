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
    :resumable          => false,
    :extension          => nil)


  def helper(name, *args, &block)
    if block_given?
      helpers[name] = block
    else
      raise RbbtException, "helper #{name} unkown in #{self} workflow" unless helpers[name]
      helpers[name].call(*args)
    end
  end

  def desc(description)
    @description = description
  end

  def extension(extension)
    @extension = extension
  end

  def resumable
    @resumable = true
  end

  def returns(description)
    @result_description = description
  end

  def dep(*dependency, &block)
    @dependencies ||= []
    dependency = [tasks.keys.last] if dependency.empty? && ! block_given?
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
        (defined? RemoteWorkflow and RemoteWorkflow === dependency.first) or
        Hash === dependency.last

        dependency = ([self] + dependency) unless Module === dependency.first || (defined?(RemoteWorkflow) && RemoteWorkflow === dependency.first)
        @dependencies << dependency
      else
        @dependencies.concat dependency
      end
    end
  end

  FORGET_DEP_TASKS = ENV["RBBT_FORGET_DEP_TASKS"] == "true"
  REMOVE_DEP_TASKS = ENV["RBBT_REMOVE_DEP_TASKS"] == "true"
  def dep_task(name, workflow, oname, *rest, &block)
    dep(workflow, oname, *rest, &block) 
    extension :dep_task unless @extension
    returns workflow.tasks[oname].result_description if workflow.tasks.include?(oname) unless @result_description 
    task name do
      raise RbbtException, "dep_task does not have any dependencies" if dependencies.empty?
      Step.wait_for_jobs dependencies.select{|d| d.streaming? }
      dep = dependencies.last
      dep.join
      raise dep.get_exception if dep.error?
      raise Aborted, "Aborted dependency #{dep.path}" if dep.aborted?
      set_info :result_type, dep.info[:result_type]
      forget = config :forget_dep_tasks, "forget_dep_tasks", :default => FORGET_DEP_TASKS
      if forget
        remove = config :remove_dep_tasks, "remove_dep_tasks", :default => REMOVE_DEP_TASKS

        self.archive_deps
        self.copy_files_dir
        self.dependencies = self.dependencies - [dep]
        Open.rm_rf self.files_dir if Open.exist? self.files_dir
        FileUtils.cp_r dep.files_dir, self.files_dir if Open.exist?(dep.files_dir)

        if dep.overriden || ! Workflow.job_path?(dep.path)
          Open.link dep.path, self.tmp_path
        else
          Open.ln_h dep.path, self.tmp_path

          case remove.to_s
          when 'true'
            dep.clean
          when 'recursive'
            (dep.dependencies + dep.rec_dependencies).uniq.each do |d|
              next if d.overriden
              d.clean unless config(:remove_dep, d.task_signature, d.task_name, d.workflow.to_s, :default => true).to_s == 'false'
            end
            dep.clean unless config(:remove_dep, dep.task_signature, dep.task_name, dep.workflow.to_s, :default => true).to_s == 'false'
          end 
        end
      else
        if Open.exists?(dep.files_dir)
          Open.rm_rf self.files_dir 
          Open.link dep.files_dir, self.files_dir
        end
        if defined?(RemoteStep) && RemoteStep === dep
          Open.write(self.tmp_path, Open.read(dep.path))
        else
          Open.link dep.path, self.path
        end
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
      :resumable          => consume_resumable,
      :input_options      => consume_input_options
    }
     
    task_info[:extension] = case task_info[:result_type].to_s
                            when "tsv"
                              "tsv"
                            when "yaml"
                              "yaml"
                            when "marshal"
                              "marshal"
                            when "json"
                              "json"
                            else
                              nil
                            end if task_info[:extension].nil?

    task = Task.setup(task_info, &block)

    last_task = task

    tasks[name] = task
    task_dependencies[name] = consume_dependencies

    task
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

  def import(source, *args)
    if args.empty?
      tasks = source.tasks.collect{|n,t| n} + source.helpers.collect{|n,h| n }
    else
      tasks = args.flatten
    end

    tasks.each do |task|
      Log.high "Task #{task} from #{source.to_s} is already present in #{self.to_s} and will be cloacked" if self.tasks.include? task.to_sym
      self.tasks[task.to_sym] = source.tasks[task.to_sym] if source.tasks.include? task.to_sym
      self.task_dependencies[task.to_sym] = source.task_dependencies[task.to_sym] if source.tasks.include? task.to_sym
      self.task_description[task.to_sym] = source.task_description[task.to_sym] if source.tasks.include? task.to_sym
      self.helpers[task.to_sym] = source.helpers[task.to_sym] if source.helpers.include? task.to_sym
    end
  end
end
