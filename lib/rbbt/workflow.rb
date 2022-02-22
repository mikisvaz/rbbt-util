require 'rbbt/workflow/definition'
require 'rbbt/workflow/dependencies'
require 'rbbt/workflow/task'
require 'rbbt/workflow/step'
require 'rbbt/workflow/accessor'
require 'rbbt/workflow/doc'
require 'rbbt/workflow/examples'

require 'rbbt/workflow/util/archive'
require 'rbbt/workflow/util/provenance'

module Workflow

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
    libdir = Path.caller_lib_dir
    return if libdir.nil?
    base.libdir = Path.setup(libdir).tap{|p| p.resource = base}
  end

  def self.init_remote_tasks
    return if defined? @@init_remote_tasks and @@init_remote_tasks
    @@init_remote_tasks = true
    load_remote_tasks(Rbbt.root.etc.remote_tasks.find) if Rbbt.root.etc.remote_tasks.exists?
  end


  def self.require_remote_workflow(wf_name, url)
    require 'rbbt/workflow/remote_workflow'
    eval "Object::#{wf_name.split("+").first} = RemoteWorkflow.new '#{ url }', '#{wf_name}'"
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

      Rbbt.add_version(filename)

      require filename
      Log.debug{"Workflow loaded from: #{ filename }"}
      return true
    rescue Exception
      Log.warn{"Error loading workflow: #{ filename }"}
      raise $!
    end
  end

  def self.installed_workflows
    self.workflow_dir['**/workflow.rb'].glob_all.collect do |file|
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

    if filename and File.exist?(filename)
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

    if Open.remote?(wf_name) or Open.ssh?(wf_name)
      url = wf_name

      if Open.ssh?(wf_name)
        wf_name = File.basename(url.split(":").last)
      else
        wf_name = File.basename(url)
      end

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
      workflow = Misc.string2const Misc.camel_case(wf_name)
      workflow.load_documentation
      return workflow
    end

    Log.high{"Loading workflow #{wf_name}"}

    first = nil
    wf_name.split("+").each do |wf_name|
      require_local_workflow(wf_name) or 
        (Workflow.autoinstall and `rbbt workflow install #{Misc.snake_case(wf_name)} || rbbt workflow install #{wf_name}` and require_local_workflow(wf_name)) or raise("Workflow not found or could not be loaded: #{ wf_name }")

      workflow = begin
                   Misc.string2const Misc.camel_case(wf_name.split("+").first)
                 rescue
                   Workflow.workflows.last || true
                 end
      workflow.load_documentation

      first ||= workflow
    end
    return first

    workflow
  end

  attr_accessor :description
  attr_accessor :libdir, :workdir 
  attr_accessor :helpers, :tasks
  attr_accessor :task_dependencies, :task_description, :last_task 
  attr_accessor :stream_exports, :asynchronous_exports, :synchronous_exports, :exec_exports
  attr_accessor :step_cache
  attr_accessor :load_step_cache
  attr_accessor :remote_tasks

  #{{{ ATTR DEFAULTS
  
  def self.workdir=(path)
    path = Path.setup path.dup unless Path === path
    @@workdir = path
  end

  def self.workdir
    @@workdir ||= if defined? Rbbt
                    Rbbt.var.jobs
                  else
                    Path.setup('var/jobs')
                  end
  end

  TAG = ENV["RBBT_INPUT_JOBNAME"] == "true" ? :inputs : :hash
  DEBUG_JOB_HASH = ENV["RBBT_DEBUG_JOB_HASH"] == 'true'
  def step_path(taskname, jobname, inputs, dependencies, extension = nil)
    raise "Jobname makes an invalid path: #{ jobname }" if jobname.include? '..'
    if inputs.length > 0 or dependencies.any?
      tagged_jobname = case TAG
                       when :hash
                         clean_inputs = Annotated.purge(inputs)
                         clean_inputs = clean_inputs.collect{|i| Symbol === i ? i.to_s : i }
                         deps_str = dependencies.collect{|d| (Step === d || (defined?(RemoteStep) && RemoteStep === Step)) ? "Step: " << (Symbol === d.overriden ? d.path : d.short_path) : d }
                         key_obj = {:inputs => clean_inputs, :dependencies => deps_str }
                         key_str = Misc.obj2str(key_obj)
                         hash_str = Misc.digest(key_str)
                         Log.debug "Hash for '#{[taskname, jobname] * "/"}' #{hash_str} for #{key_str}" if DEBUG_JOB_HASH
                         jobname + '_' << hash_str
                       when :inputs
                         all_inputs = {}
                         inputs.zip(self.task_info(taskname)[:inputs]) do |i,f|
                           all_inputs[f] = i
                         end
                         dependencies.each do |dep|
                           ri = dep.recursive_inputs
                           ri.zip(ri.fields).each do |i,f|
                             all_inputs[f] = i
                           end
                         end

                         all_inputs.any? ? jobname + '_' << Misc.obj2str(all_inputs) : jobname
                       else
                         jobname
                       end
    else
      tagged_jobname = jobname
    end

    if extension and not extension.empty?
      tagged_jobname = tagged_jobname + ('.' << extension.to_s)
    end

    workdir[taskname][tagged_jobname].find
  end
  def import_task(workflow, orig, new)
    orig_task = workflow.tasks[orig]
    new_task = orig_task.dup
    options = {}
    orig_task.singleton_methods.
      select{|method| method.to_s[-1] != "="[0]}.each{|method|
      if orig_task.respond_to?(method.to_s + "=") 
        options[method.to_s] = orig_task.send(method.to_s)
      end
    }

    Task.setup(options, &new_task)
    new_task.workflow = self
    new_task.name = new
    tasks[new] = new_task
    task_dependencies[new] = workflow.task_dependencies[orig]
    task_description[new] = workflow.task_description[orig]
  end

  def workdir=(path)
    path = Path.setup path.dup unless Path === path
    @workdir = path
  end

  def workdir
    @workdir ||= begin
                   text = Module === self ? self.to_s : "Misc"
                   Workflow.workdir[text]
                 end
  end

  def libdir
    @libdir = Path.setup(Path.caller_lib_dir) if @libdir.nil?
    @libdir 
  end

  def step_cache
    Thread.current[:step_cache] ||= {}
  end

  def self.load_step_cache
    Thread.current[:load_step_cache] ||= {}
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
      if (String === value and value =~ /^local:(.*?):(.*)/) or 
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

  def __job(taskname, jobname = nil, inputs = {})
    taskname = taskname.to_sym
    return remote_tasks[taskname].job(taskname, jobname, inputs) if remote_tasks and remote_tasks.include? taskname

    task = tasks[taskname]
    raise "Task not found: #{ taskname }" if task.nil?

    inputs = IndiferentHash.setup(inputs)

    not_overriden = inputs.delete :not_overriden 
    if not_overriden
      inputs[:not_overriden] = :not_overriden_dep
    end

    Workflow.resolve_locals(inputs)

    task_info = task_info(taskname)
    task_inputs = task_info[:inputs]
    #defaults = IndiferentHash.setup(task_info[:input_defaults]).merge(task.input_defaults)
    all_defaults = IndiferentHash.setup(task_info[:input_defaults])
    defaults = IndiferentHash.setup(task.input_defaults)

    missing_inputs = []
    task.required_inputs.each do |input|
      missing_inputs << input if inputs[input].nil?
    end if task.required_inputs

    if missing_inputs.length == 1
      raise ParameterException, "Input #{missing_inputs.first} is required but was not provided or is nil"
    end

    if missing_inputs.length > 1
      raise ParameterException, "Inputs #{Misc.humanize_list(missing_inputs)} are required but were not provided or are nil"
    end

    # jobname => true sets the value of the input to the name of the job
    if task.input_options
      jobname_input = task.input_options.select{|i,o| o[:jobname] }.collect{|i,o| i }.first
    else
      jobname_input = nil 
    end

    if jobname_input && jobname && inputs[jobname_input].nil?
      inputs[jobname_input] = jobname
    end

    real_inputs = {}
    has_overriden_inputs = false

    inputs.each do |k,v|
      #has_overriden_inputs = true if String === k and k.include? "#"
      next unless (task_inputs.include?(k.to_sym) or task_inputs.include?(k.to_s))
      default = all_defaults[k]
      next if default == v 
      next if (String === default and Symbol === v and v.to_s == default)
      next if (Symbol === default and String === v and v == default.to_s)
      real_inputs[k.to_sym] = v 
    end

    jobname_input_value = inputs[jobname_input] || all_defaults[jobname_input]
    if jobname_input && jobname.nil? && String === jobname_input_value && ! jobname_input_value.include?('/')
      jobname = jobname_input_value
    end

    jobname = DEFAULT_NAME if jobname.nil? or jobname.empty?

    dependencies = real_dependencies(task, jobname, defaults.merge(inputs), task_dependencies[taskname] || [])

    overriden_deps = dependencies.select{|d| d.overriden }
    true_overriden_deps = overriden_deps.select{|d| TrueClass === d.overriden }

    overriden = has_overriden_inputs || overriden_deps.any?

    extension = task.extension

    if extension == :dep_task
      extension = nil
      if dependencies.any?
        dep_basename = File.basename(dependencies.last.path)
        if dep_basename.include? "."
          parts = dep_basename.split(".")
          extension = [parts.pop]
          while parts.last.length <= 4
            extension << parts.pop
          end
          extension = extension.reverse * "."
        end
      end
    end

    input_values = task.take_input_values(inputs)
    if real_inputs.empty? && Workflow::TAG != :inputs && ! overriden 
      step_path = step_path taskname, jobname, [], [], extension
    else
      step_path = step_path taskname, jobname, input_values, dependencies, extension
    end


    job = get_job_step step_path, task, input_values, dependencies
    job.workflow = self
    job.clean_name = jobname

    case not_overriden 
    when TrueClass
      job.overriden = has_overriden_inputs || true_overriden_deps.any?
    when :not_overriden_dep
      job.overriden = true if has_overriden_inputs || true_overriden_deps.any?
    else
      job.overriden = true if has_overriden_inputs || overriden_deps.any?
    end

    job.real_inputs = real_inputs.keys
    job
  end

  def _job(taskname, jobname = nil, inputs = {})

    task_info = task_info(taskname)
    task_inputs = task_info[:inputs]
    persist_inputs = inputs.values_at(*task_inputs)
    persist_inputs += inputs.values_at(*inputs.keys.select{|k| String === k && k.include?("#") }.sort)
    Persist.memory("STEP", :workflow => self.to_s, :taskname => taskname, :jobname => jobname, :inputs => persist_inputs, :repo => step_cache) do
      __job(taskname, jobname, inputs)
    end
  end

  def job(taskname, jobname = nil, inputs = {})
    begin
      _job(taskname, jobname, inputs)
    ensure
      step_cache.clear
    end
  end


  def set_step_dependencies(step)
    if step.info[:dependencies]
      Misc.insist do
        step.dependencies = step.info[:dependencies].collect do |task, job, path|
          next if job.nil?
          if Open.exists?(path)
            load_step(path) 
          else
            Workflow.load_step(path)
          end
        end
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

  def load_step(path)
    task = task_for path
    if task
      get_job_step path, tasks[task.to_sym]
    else
      get_job_step path
    end
  end

  def self.transplant(listed, real, other)
    if listed.nil?
      parts = real.split("/")
      other_parts = other.split("/")
      listed = (other_parts[0..-4] + parts[-3..-1]) * "/"
    end

    sl = listed.split("/", -1)
    so = other.split("/", -1)
    sr = real.split("/", -1)
    prefix = [] 
    while true
      break if sl[0] != so[0]
      cl = sl.shift
      co = so.shift
      prefix << cl
    end
    File.join(sr - sl + so)
  end

  def  self.relocate_array(real, list)
    preal = real.split(/\/+/)
    prefix = preal[0..-4] * "/" 
    list.collect do |other|
      pother = other.split(/\/+/)
      end_part = pother[-3..-1] * "/"
      new_path = prefix + "/" << end_part
      if File.exists? new_path
        new_path 
      else
        Rbbt.var.jobs[end_part].find
      end
    end
  end

  def  self.relocate(real, other)
    preal = real.split(/\/+/)
    pother = other.split(/\/+/)
    end_part = pother[-3..-1] * "/"
    new_path = preal[0..-4] * "/" << "/" << end_part
    return new_path if File.exists?(new_path) || File.exists?(new_path + '.info')
    Rbbt.var.jobs[end_part].find
  end

  def self.relocate_dependency(main, dep)
    dep_path = dep.path
    path = main.path
    if Open.exists?(dep_path) || Open.exists?(dep_path + '.info')
      dep
    else
      new_path = relocate(path, dep_path)
      relocated = true if Open.exists?(new_path) || Open.exists?(new_path + '.info')
      Workflow._load_step new_path
    end
  end

  def self.__load_step(path)
    if Open.remote?(path) || Open.ssh?(path)
      require 'rbbt/workflow/remote_workflow'
      return RemoteWorkflow.load_path path
    end
    step = Step.new path
    relocated = false
    step.dependencies = (step.info[:dependencies] || []).collect do |task,name,dep_path|
      if Open.exists?(dep_path) || Open.exists?(dep_path + '.info') || Open.remote?(dep_path) || Open.ssh?(dep_path)
        Workflow._load_step dep_path
      else
        new_path = relocate(path, dep_path)
        relocated = true if Open.exists?(new_path) || Open.exists?(new_path + '.info')
        Workflow._load_step new_path
      end
    end
    step.relocated = relocated
    step.load_inputs_from_info
    step
  end
    
  def self.fast_load_step(path)
    if Open.remote?(path) || Open.ssh?(path)
      require 'rbbt/workflow/remote_workflow'
      return RemoteWorkflow.load_path path
    end

    step = Step.new path
    step.dependencies = nil
    class << step
      def dependencies
        @dependencies ||= (self.info[:dependencies] || []).collect do |task,name,dep_path|
          dep = if Open.exists?(dep_path) || Open.exists?(dep_path + '.info')
                  relocate = false
                  Workflow.fast_load_step dep_path
                else
                  new_path = Workflow.relocate(path, dep_path)
                  relocated = true if Open.exists?(new_path) || Open.exists?(new_path + '.info')
                  Workflow.fast_load_step new_path
                end
          dep.relocated = relocated
          dep
        end
        @dependencies
      end

      def inputs
        self.load_inputs_from_info unless @inputs
        @inputs
      end

      def dirty?
        false
      end

      def updated?
        true
      end
    end

    if ! Open.exists?(step.info_file)
      begin
        workflow = step.path.split("/")[-3]
        task_name = step.path.split("/")[-2]
        workflow = Kernel.const_get workflow
        step.task = workflow.tasks[task_name.to_sym]
      rescue
        Log.exception $!
      end
    end
    step
  end

  def self._load_step(path)
    Persist.memory("STEP", :path => path, :repo => load_step_cache) do
      __load_step(path)
    end
  end

  def self.load_step(path)
    path = Path.setup(path.dup) unless Path === path
    path = path.find

    begin
      _load_step(path)
    ensure
      load_step_cache.clear
    end
  end

  def load_id(id)
    path = if Path === workdir
             workdir[id].find
           else
             File.join(workdir, id)
           end
    task = task_for path
    return remote_tasks[task].load_id(id) if remote_tasks && remote_tasks.include?(task)
    return Workflow.load_step path
  end


  def fast_load_id(id)
    path = if Path === workdir
             workdir[id].find
           else
             File.join(workdir, id)
           end
    task = task_for path
    return remote_tasks[task].load_id(id) if remote_tasks && remote_tasks.include?(task)
    return Workflow.fast_load_step path
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
    require 'rbbt/workflow/remote_workflow'
    remote_tasks.each do |workflow, info|
      wf = Workflow.require_workflow workflow
      wf.remote_tasks ||= {}
      IndiferentHash.setup wf.remote_tasks
      info.each do |remote, tasks|
        remote_wf = RemoteWorkflow.new remote, workflow
        tasks.each do |task|
          Log.debug "Add remote task #{task} in #{wf} using #{remote_wf.url}"
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
