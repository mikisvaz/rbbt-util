require 'rbbt/workflow/definition'
require 'rbbt/workflow/task'
require 'rbbt/workflow/step'
require 'rbbt/workflow/accessor'

module Workflow
  def self.resolve_locals(inputs)
    inputs.each do |name, value|
      if value =~ /^local:(.*?):(.*)/ or 
        (Array === value and value.length == 1 and value.first =~ /^local:(.*?):(.*)/) or
        (TSV === value and value.size == 1 and value.keys.first =~ /^local:(.*?):(.*)/)
        task_name = $1
        jobname = $2
        value = load_id(File.join(task_name, jobname)).load
      end
      inputs[name] = value
    end 
  end

  #{{{ WORKFLOW MANAGEMENT 
  class << self
    attr_accessor :workflows
  end
  self.workflows = []

  def self.extended(base)
    self.workflows << base
    base.libdir = Path.caller_lib_dir.tap{|p| p.resource = base}
  end

  def self.require_remote_workflow(wf_name, url)
    require 'rbbt/workflow/rest/client'
    eval "Object::#{wf_name} = RbbtRestClient.new '#{ url }', '#{wf_name}'"
  end

  def self.require_local_workflow(wf_name)
    if Path === wf_name
      case

        # Points to workflow file
      when ((File.exists?(wf_name.find) and not File.directory?(wf_name.find)) or File.exists?(wf_name.find + '.rb')) 
        $LOAD_PATH.unshift(File.join(File.expand_path(File.dirname(wf_name.find)), 'lib'))
        require wf_name.find
        Log.debug "Workflow loaded from file: #{ wf_name }"
        return true

        # Points to workflow dir
      when (File.exists?(wf_name.find) and File.directory?(wf_name.find) and File.exists?(File.join(wf_name.find, 'workflow.rb')))
        $LOAD_PATH.unshift(File.join(File.expand_path(wf_name.find), 'lib'))
        require File.join(wf_name.find, 'workflow.rb')
        Log.debug "Workflow loaded from directory: #{ wf_name }"
        return true

      else
        raise "Workflow path was not resolved: #{ wf_name } (#{wf_name.find})"
      end

    else
      case
        # Points to workflow file
      when ((File.exists?(wf_name) and not File.directory?(wf_name)) or File.exists?(wf_name + '.rb')) 
        $LOAD_PATH.unshift(File.join(File.expand_path(File.dirname(wf_name)), 'lib'))
        require wf_name
        Log.debug "Workflow loaded from file: #{ wf_name }"
        return true

      when (defined?(Rbbt) and Rbbt.etc.workflow_dir.exists?)
        dir = Rbbt.etc.workflow_dir.read.strip
        dir = File.join(dir, wf_name)
        $LOAD_PATH.unshift(File.join(File.expand_path(dir), 'lib'))
        require File.join(dir, 'workflow.rb')
        Log.debug "Workflow #{wf_name} loaded from workflow_dir: #{ dir }"
        return true

      when defined?(Rbbt)
        path = Rbbt.workflows[wf_name].find
        $LOAD_PATH.unshift(File.join(File.expand_path(path), 'lib'))
        require File.join(path, 'workflow.rb')
        Log.debug "Workflow #{wf_name} loaded from Rbbt.workflows: #{ path }"
        return true

      else
        path = File.join(ENV['HOME'], '.workflows', wf_name)
        $LOAD_PATH.unshift(File.join(File.expand_path(path), 'lib'))
        require File.join(path, 'workflow.rb')
        Log.debug "Workflow #{wf_name} loaded from .workflows: #{ path }"
        return true
      end
    end

    raise "Workflow not found our could not be loaded: #{ wf_name }"
  end

  def self.require_workflow(wf_name)
    begin
      Misc.string2const wf_name
      Log.debug "Workflow #{ wf_name } already loaded"
      return true
    rescue Exception
    end

    if Rbbt.etc.remote_workflows.exists?
      remote_workflows = Rbbt.etc.remote_workflows.yaml
      if remote_workflows.include? wf_name
        url = remote_workflows[wf_name]
        require_remote_workflow(wf_name, url)
        Log.debug "Workflow #{ wf_name } loaded remotely: #{ url }"
        return
      end
    end

    begin
      require_local_workflow(wf_name) 
    rescue Exception
      Log.debug $!.message 
      Log.debug $!.backtrace.first
      raise "Workflow not found: #{ wf_name }" if wf_name == Misc.snake_case(wf_name)
      Log.debug "Trying with humanized: '#{Misc.snake_case wf_name}'"
      begin
        require_local_workflow(Misc.snake_case(wf_name))
      rescue Exception
        Log.debug $!.message
        raise "Workflow not found: #{ wf_name }"
      end
    end
  end

  attr_accessor :libdir, :workdir 
  attr_accessor :helpers, :tasks
  attr_accessor :task_dependencies, :task_description, :last_task 
  attr_accessor :asynchronous_exports, :synchronous_exports, :exec_exports

  #{{{ ATTR DEFAULTS

  def workdir
    @workdir ||= if defined? Rbbt
                   Rbbt.var.jobs[self].find
                 else
                   Path.setup('var/jobs')
                 end
  end

  def libdir
    @libdir = Path.caller_lib_dir if @libdir.nil?
    @libdir 
  end

  def helpers
    @helpers ||= {}
  end

  def tasks
    @tasks ||= {} 
  end

  def task_dependencies
    @task_dependencies ||= {} 
  end

  def task_description
    @task_description ||= {}
  end

  def asynchronous_exports
    @asynchronous_exports ||= []
  end

  def synchronous_exports
    @synchronous_exports ||= []
  end

  def exec_exports
    @exec_exports ||= []
  end

  # {{{ JOB MANAGEMENT

  def job(taskname, jobname = nil, inputs = {})
    taskname = taskname.to_sym
    jobname = "Default" if jobname.nil? or jobname.empty?
    task = tasks[taskname]
    raise "Task not found: #{ taskname }" if task.nil?

    IndiferentHash.setup(inputs)

    Workflow.resolve_locals(inputs)

    dependencies = real_dependencies(task, jobname, inputs, task_dependencies[taskname] || [])

    input_values = task.take_input_values(inputs)

    step_path = step_path taskname, jobname, input_values, dependencies

    step = Step.new step_path, task, input_values, dependencies

    helpers.each do |name, block|
      (class << step; self; end).instance_eval do
        define_method name, &block
      end
    end

    step
  end

  def load_step(path)
    task = task_for path
    Step.new path, tasks[task.to_sym]
  end

  def load_id(id)
    path = File.join(workdir, id)
    task = task_for path
    step = Step.new path, tasks[task.to_sym]
    if step.info.include? :dependencies
      step.dependencies = step.info[:dependencies].collect do |task, job|
        load_id(File.join(task.to_s, job))
      end
    end
    step
  end

  def jobs(task, query = nil)
    task_dir = File.join(workdir.find, task.to_s)
    if query.nil?
      path = File.join(task_dir, "**/*.info")
    else
      path = File.join(task_dir, query + "*.info")
    end

    Dir.glob(path).collect{|f|
      Misc.path_relative_to(task_dir, f).sub(".info",'')
    }
  end
end
