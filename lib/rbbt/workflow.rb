require 'rbbt/workflow/definition'
require 'rbbt/workflow/task'
require 'rbbt/workflow/step'
require 'rbbt/workflow/accessor'

module Workflow

  #{{{ WORKFLOW MANAGEMENT 
  class << self
    attr_accessor :workflows, :autoinstall
  end

  self.workflows = []

  def self.autoinstall
    @autoload ||= ENV["RBBT_WORKFLOW_AUTOINSTALL"] == "true"
  end
  def self.extended(base)
    self.workflows << base
    base.libdir = Path.setup(Path.caller_lib_dir).tap{|p| p.resource = base}
  end

  def self.require_remote_workflow(wf_name, url)
    require 'rbbt/rest/client'
    eval "Object::#{wf_name} = WorkflowRESTClient.new '#{ url }', '#{wf_name}'"
  end

  def self.load_workflow_file(filename)
    begin
      $LOAD_PATH.unshift(File.join(File.dirname(File.expand_path(filename)), 'lib'))
      require filename
      Log.debug{"Workflow loaded from: #{ filename }"}
      return true
    rescue Exception
      Log.warn{"Error loading workflow: #{ filename }"}
      raise $!
    end
  end

  def self.require_local_workflow(wf_name)
    filename = nil

    if Path === wf_name
      case
        # Points to workflow file
      when ((File.exists?(wf_name.find) and not File.directory?(wf_name.find)) or File.exists?(wf_name.find + '.rb')) 
        filename = wf_name.find

        # Points to workflow dir
      when (File.exists?(wf_name.find) and File.directory?(wf_name.find) and File.exists?(File.join(wf_name.find, 'workflow.rb')))
        filename = wf_name['workflow.rb'].find
      end

    else
      case
        # Points to workflow file
      when ((File.exists?(wf_name) and not File.directory?(wf_name)) or File.exists?(wf_name + '.rb'))
        filename = (wf_name =~ /\.?\//) ? wf_name : "./" << wf_name 
      when (defined?(Rbbt) and Rbbt.etc.workflow_dir.exists?)
        dir = Rbbt.etc.workflow_dir.read.strip
        dir = File.join(dir, wf_name)
        filename = File.join(dir, 'workflow.rb')
      when defined?(Rbbt)
        path = Rbbt.workflows[wf_name].find
        filename = File.join(path, 'workflow.rb')
      else
        path = File.join(ENV['HOME'], '.workflows', wf_name)
        filename = File.join(dir, 'workflow.rb')
      end
    end

    if filename and File.exists? filename
      load_workflow_file filename
    else
      return false
    end
  end

  def self.require_workflow(wf_name)

    # Already loaded
    begin
      Misc.string2const wf_name
      Log.debug{"Workflow #{ wf_name } already loaded"}
      return true
    rescue Exception
    end

    # Load remotely
    if Rbbt.etc.remote_workflows.exists?
      remote_workflows = Rbbt.etc.remote_workflows.yaml
      if Hash === remote_workflows and remote_workflows.include?(wf_name)
        url = remote_workflows[wf_name]
        require_remote_workflow(wf_name, url)
        Log.debug{"Workflow #{ wf_name } loaded remotely: #{ url }"}
        return
      end
    end

    # Load locally

    Log.info{"Loading workflow #{wf_name}"}
    require_local_workflow(wf_name) or 
    require_local_workflow(Misc.snake_case(wf_name)) or 
    (Workflow.autoinstall and `rbbt workflow install #{Misc.snake_case(wf_name)}` and require_local_workflow(Misc.snake_case(wf_name))) or
    raise("Workflow not found or could not be loaded: #{ wf_name }")
  end

  attr_accessor :description
  attr_accessor :libdir, :workdir 
  attr_accessor :helpers, :tasks
  attr_accessor :task_dependencies, :task_description, :last_task 
  attr_accessor :asynchronous_exports, :synchronous_exports, :exec_exports

  #{{{ ATTR DEFAULTS

  def workdir
    @workdir ||= if defined? Rbbt
                   text = Module === self ? self.to_s : "Misc"
                   Rbbt.var.jobs[text].find
                 else
                   Path.setup('var/jobs')
                 end
  end

  def libdir
    @libdir = Path.caller_lib_dir if @libdir.nil?
    @libdir 
  end
  
  def workflow_description
    @workflow_description ||= begin
                       file = @libdir['workflow.md']
                       if file.exists?
                         file.read
                       else
                         ""
                       end
                     end
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
  DEFAULT_NAME="Default"

  def self.resolve_locals(inputs)
    inputs.each do |name, value|
      if value =~ /^local:(.*?):(.*)/ or 
        (Array === value and value.length == 1 and value.first =~ /^local:(.*?):(.*)/) or
        (TSV === value and value.size == 1 and value.keys.first =~ /^local:(.*?):(.*)/)
        task_name = $1
        jobname = $2
        value = load_id(File.join(task_name, jobname)).load
        inputs[name] = value
      end
    end 
  end

  def job(taskname, jobname = nil, inputs = {})
    taskname = taskname.to_sym
    jobname = DEFAULT_NAME if jobname.nil? or jobname.empty?

    task = tasks[taskname]
    raise "Task not found: #{ taskname }" if task.nil?

    IndiferentHash.setup(inputs)

    Workflow.resolve_locals(inputs)

    dependencies = real_dependencies(task, jobname, inputs, task_dependencies[taskname] || [])

    if inputs.empty?
      step_path = step_path taskname, jobname, [], []
      input_values = task.take_input_values(inputs)
    else
      input_values = task.take_input_values(inputs)
      step_path = step_path taskname, jobname, input_values, dependencies
    end

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
    step.info
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

  #{{{ Make workflow resources local
  def local_persist_setup
    class << self
      include LocalPersist
    end
    self.local_persist_dir = Rbbt.var.cache.persistence.find :lib
  end

  def local_workdir_setup
    self.workdir = Rbbt.var.jobs.find :lib
  end

  def make_local
    local_persist_setup
    local_workdir_setup
  end
end
