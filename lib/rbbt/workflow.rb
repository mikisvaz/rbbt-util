require 'rbbt/workflow/definition'
require 'rbbt/workflow/task'
require 'rbbt/workflow/step'
require 'rbbt/workflow/accessor'
require 'rbbt/workflow/doc'
require 'rbbt/workflow/examples'

module Workflow

  STEP_CACHE = {}

  class TaskNotFoundException < Exception 
    def initialize(workflow, task = nil)
      if task
        super "Task '#{ task }' not found in #{ workflow } workflow"
      else
        super workflow
      end
    end
  end

  #{{{ WORKFLOW MANAGEMENT 
  class << self
    attr_accessor :workflows, :autoinstall, :workflow_dir
  end

  self.workflows = []

  def self.autoinstall
    @autoload ||= ENV["RBBT_WORKFLOW_AUTOINSTALL"] == "true"
  end

  def self.extended(base)
    self.workflows << base
    base.libdir = Path.setup(Path.caller_lib_dir).tap{|p| p.resource = base}
  end

  def self.init_remote_tasks
    return if defined? @@init_remote_tasks and @@init_remote_tasks
    @@init_remote_tasks = true
    load_remote_tasks(Rbbt.root.etc.remote_tasks.find) if Rbbt.root.etc.remote_tasks.exists?
  end

  def self.require_remote_workflow(wf_name, url)
    require 'rbbt/rest/client'
    eval "Object::#{wf_name} = WorkflowRESTClient.new '#{ url }', '#{wf_name}'"
  end

  def self.load_workflow_libdir(filename)
    workflow_lib_dir = File.join(File.dirname(File.expand_path(filename)), 'lib')
    if File.directory? workflow_lib_dir
      Log.debug "Adding workflow lib directory to LOAD_PATH: #{workflow_lib_dir}"
      $LOAD_PATH.unshift(workflow_lib_dir)
    end
  end

  def self.load_workflow_file(filename)
    begin

      load_workflow_libdir(filename)

      filename = File.expand_path(filename)

      require filename
      Log.debug{"Workflow loaded from: #{ filename }"}
      return true
    rescue Exception
      Log.warn{"Error loading workflow: #{ filename }"}
      raise $!
    end
  end

  def self.installed_workflows
    self.workflow_dir.glob('**/workflow.rb').collect do |file|
      File.basename(File.dirname(file))
    end
  end

  def self.workflow_dir
    @workflow_dir ||= begin
                        case
                        when (defined?(Rbbt) and Rbbt.etc.workflow_dir.exists?)
                          dir = Rbbt.etc.workflow_dir.read.strip
                          dir = File.expand_path(dir)
                          Path.setup(dir)
                        when defined?(Rbbt)
                          Rbbt.workflows
                        else
                          dir = File.join(ENV['HOME'], '.workflows')
                          Path.setup(dir)
                        end
                      end
  end

  def self.local_workflow_filename(wf_name)
    filename = nil

    if Path === wf_name
      case
        # Points to workflow file
      when ((File.exist?(wf_name.find) and not File.directory?(wf_name.find)) or File.exist?(wf_name.find + '.rb')) 
        filename = wf_name.find

        # Points to workflow dir
      when (File.exist?(wf_name.find) and File.directory?(wf_name.find) and File.exist?(File.join(wf_name.find, 'workflow.rb')))
        filename = wf_name['workflow.rb'].find
      end

    else
      if ((File.exist?(wf_name) and not File.directory?(wf_name)) or File.exist?(wf_name + '.rb'))
        filename = (wf_name =~ /\.?\//) ? wf_name : "./" << wf_name 
      else
        filename = workflow_dir[wf_name]['workflow.rb'].find
      end
    end

    if filename.nil? or not File.exist?(filename)
      wf_name_snake = Misc.snake_case(wf_name)
      return local_workflow_filename(wf_name_snake) if wf_name_snake != wf_name
    end

    filename
  end

  def self.require_local_workflow(wf_name)
    filename = local_workflow_filename(wf_name)

    if filename and File.exist? filename
      load_workflow_file filename
    else
      return false
    end
  end

  def self.require_workflow(wf_name, force_local=false)
    Workflow.init_remote_tasks
    # Already loaded
    begin
      workflow = Misc.string2const wf_name
      Log.debug{"Workflow #{ wf_name } already loaded"}
      return workflow
    rescue Exception
    end

    # Load remotely
    if not force_local and Rbbt.etc.remote_workflows.exists?
      remote_workflows = Rbbt.etc.remote_workflows.yaml
      if Hash === remote_workflows and remote_workflows.include?(wf_name)
        url = remote_workflows[wf_name]
        begin
          return require_remote_workflow(wf_name, url)
        ensure
          Log.debug{"Workflow #{ wf_name } loaded remotely: #{ url }"}
        end
      end
    end

    if Open.remote? wf_name
      url = wf_name
      wf_name = File.basename(url)
      begin
        return require_remote_workflow(wf_name, url)
      ensure
        Log.debug{"Workflow #{ wf_name } loaded remotely: #{ url }"}
      end
    end

    # Load locally

    if wf_name =~ /::\w+$/
      clean_name = wf_name.sub(/::.*/,'')  
      Log.info{"Looking for '#{wf_name}' in '#{clean_name}'"}
      require_workflow clean_name
      return Misc.string2const Misc.camel_case(wf_name)
    end

    Log.info{"Loading workflow #{wf_name}"}
    require_local_workflow(wf_name) or 
    (Workflow.autoinstall and `rbbt workflow install #{Misc.snake_case(wf_name)}` and require_local_workflow(Misc.snake_case(wf_name))) or
    raise("Workflow not found or could not be loaded: #{ wf_name }")
    begin
      Misc.string2const Misc.camel_case(wf_name)
    rescue
      Workflow.workflows.last || true
    end
  end

  attr_accessor :description
  attr_accessor :libdir, :workdir 
  attr_accessor :helpers, :tasks
  attr_accessor :task_dependencies, :task_description, :last_task 
  attr_accessor :stream_exports, :asynchronous_exports, :synchronous_exports, :exec_exports
  attr_accessor :step_cache
  attr_accessor :remote_tasks

  #{{{ ATTR DEFAULTS
  
  def self.workdir=(path)
    path = Path.setup path.dup unless Path === path
    @@workdir = path
  end

  def self.workdir
    @@workdir ||= if defined? Rbbt
                   Rbbt.var.jobs.find
                 else
                   Path.setup('var/jobs')
                 end
  end

  def workdir=(path)
    path = Path.setup path.dup unless Path === path
    @workdir = path
  end

  def workdir
    @workdir ||= begin
                   text = Module === self ? self.to_s : "Misc"
                   Workflow.workdir[text].find
                 end
  end

  def libdir
    @libdir = Path.caller_lib_dir if @libdir.nil?
    @libdir 
  end

  def step_cache
    @step_cache ||= Workflow::STEP_CACHE
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

  def stream_exports
    @stream_exports ||= []
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
  
  def all_exports
    @all_exports ||= asynchronous_exports + synchronous_exports + exec_exports + stream_exports
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

  def step_module
    @_m ||= begin
              m = Module.new

              helpers.each do |name,block|
                m.send(:define_method, name, &block)
              end

              m
            end
    @_m
  end

  def job(taskname, jobname = nil, inputs = {})
    taskname = taskname.to_sym
    return remote_tasks[taskname].job(taskname, jobname, inputs) if remote_tasks and remote_tasks.include? taskname

    jobname = DEFAULT_NAME if jobname.nil? or jobname.empty?

    task = tasks[taskname]
    raise "Task not found: #{ taskname }" if task.nil?

    inputs = IndiferentHash.setup(inputs)

    Workflow.resolve_locals(inputs)

    task_info = task_info(taskname)
    task_inputs = task_info[:inputs]
    defaults = IndiferentHash.setup(task_info[:input_defaults]).merge(task.input_defaults)

    missing_inputs = []
    task.required_inputs.each do |input|
      missing_inputs << input if inputs[input].nil?
    end

    if missing_inputs.length == 1
      raise ParameterException, "Input #{missing_inputs.first} is required but was not provided or is nil"
    end

    if missing_inputs.length > 1
      raise ParameterException, "Inputs #{Misc.humanize_list(missing_inputs)} are required but were not provided or are nil"
    end

    dependencies = real_dependencies(task, jobname, defaults.merge(inputs), task_dependencies[taskname] || [])

    real_inputs = {}
    recursive_inputs = rec_inputs(taskname)

    inputs.each do |k,v|
      default = defaults[k]
      next unless (task_inputs.include?(k.to_sym) or task_inputs.include?(k.to_s))
      next if default == v 
      next if (String === default and Symbol === v and v.to_s == default)
      next if (Symbol === default and String === v and v == default.to_s)
      real_inputs[k] = v 
    end

    if real_inputs.empty? and not Workflow::TAG == :inputs
      step_path = step_path taskname, jobname, [], [], task.extension
      input_values = task.take_input_values(inputs)
    else
      input_values = task.take_input_values(inputs)
      step_path = step_path taskname, jobname, input_values, dependencies, task.extension
    end

    job = get_job_step step_path, task, input_values, dependencies
    job.workflow = self
    job.clean_name = jobname
    job
  end

  def set_step_dependencies(step)
    if step.info.include? :dependencies
      step.dependencies = step.info[:dependencies].collect do |task, job, path|
        next if job.nil?
        load_step(path) 
      end
    end
  end

  #{{{ LOAD FROM FILE

  def get_job_step(step_path, task = nil, input_values = nil, dependencies = nil)
    step_path = step_path.call if Proc === step_path
    persist = input_values.nil? ? false : true
    persist = false
    key = Path === step_path ? step_path.find : step_path

    step = Step.new step_path, task, input_values, dependencies

    set_step_dependencies(step) unless dependencies

    step.extend step_module

    step.task ||= task
    step.inputs ||= input_values
    step.dependencies = dependencies if dependencies and (step.dependencies.nil? or step.dependencies.length < dependencies.length)


    step
  end

  def load_id(id)
    path = File.join(workdir, id)
    task = task_for path
    return remote_tasks[task].load_id(id) if remote_tasks and remote_tasks.include? task
    step = Step.new path, tasks[task.to_sym]
    step.load_inputs_from_info
    set_step_dependencies(step)
    step
  end

  def load_step(path)
    task = task_for path
    if task
      get_job_step path, tasks[task.to_sym]
    else
      get_job_step path
    end
  end

  def load_name(task, name)
    return remote_tasks[task].load_step(path) if remote_tasks and remote_tasks.include? task
    task = tasks[task.to_sym] if String === task or Symbol === task
    path = step_path task.name, name, [], [], task.extension
    get_job_step path, task
  end

  #}}} LOAD FROM FILE
  
  def jobs(taskname, query = nil)
    task_dir = File.join(File.expand_path(workdir.find), taskname.to_s)
    pattern = File.join(File.expand_path(task_dir), '**/*')
    job_info_files = Dir.glob(Step.info_file(pattern)).collect{|f| Misc.path_relative_to task_dir, f }
    job_info_files = job_info_files.select{|f| f.index(query) == 0 } if query
    job_info_files.collect{|f|
      job_name = Step.job_name_for_info_file(f, tasks[taskname].extension)
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

  def with_workdir(workdir)
    saved = self.workdir
    begin
      self.workdir = Path.setup(File.expand_path(workdir))
      yield
    ensure
      self.workdir = saved
    end
  end

  def add_remote_tasks(remote_tasks)
    remote_tasks.each do |remote, tasks|
      tasks.each do |task|
        self.remote_tasks[task.to_f] = remote
      end
    end
  end

  def self.process_remote_tasks(remote_tasks)
    require 'rbbt/rest/client'
    remote_tasks.each do |workflow, info|
      wf = Workflow.require_workflow workflow
      wf.remote_tasks ||= {}
      IndiferentHash.setup wf.remote_tasks
      info.each do |remote, tasks|
        remote_wf = WorkflowRESTClient.new remote, workflow
        tasks.each do |task|
          wf.remote_tasks[task.to_sym] = remote_wf
        end
      end
    end
  end

  def self.load_remote_tasks(filename)
    yaml_text = Open.read(filename)
    remote_workflow_tasks = YAML.load(yaml_text)
    Workflow.process_remote_tasks(remote_workflow_tasks)
  end

end
