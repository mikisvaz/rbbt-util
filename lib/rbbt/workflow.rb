require 'rbbt/workflow/task'
require 'rbbt/workflow/step'
require 'rbbt/workflow/annotate'
require 'rbbt/workflow/accessor'

module Workflow
  class << self
    attr_accessor :workflows
  end
  self.workflows = []

  def self.require_workflow(wf_name, wf_dir = nil)
    require 'rbbt/resource/path'

    if File.exists?(wf_name) or File.exists?(wf_name + '.rb')
      $LOAD_PATH.unshift(File.join(File.expand_path(File.dirname(wf_name)), 'lib'))
      require wf_name
      return
    end

    wf_dir ||= case
               when File.exists?(File.join(File.dirname(Path.caller_lib_dir), wf_name))
                 dir = File.join(File.dirname(Path.caller_lib_dir), wf_name)
                 Log.debug "Loading workflow from lib dir: #{dir}"
                 dir
                 File.join(File.dirname(Path.caller_lib_dir), wf_name)
               when defined? Rbbt
                 if Rbbt.etc.workflow_dir.exists?
                   dir = File.join(Rbbt.etc.workflow_dir.read.strip, wf_name)
                   Log.debug "Loading workflow from etc dir: #{dir}"
                   dir
                 else
                   dir = Rbbt.workflows[wf_name]
                   Log.debug "Loading workflow from main dir: #{dir}"
                   dir
                 end
               else
                 dir = File.join(ENV["HOME"], '.workflows')
                 Log.debug "Loading workflow from home dir: #{dir}"
                 dir
               end

    wf_dir = Path.setup(wf_dir)

    $LOAD_PATH.unshift(File.join(File.dirname(wf_dir["workflow.rb"].find), 'lib'))
    require wf_dir["workflow.rb"].find
  end

  def self.extended(base)
    if not base.respond_to? :workdir
      base.extend AnnotatedModule
      class << base
        attr_accessor :libdir, :workdir, :tasks, :task_dependencies, :task_description, :dependencies, :asynchronous_exports, :synchronous_exports, :exec_exports, :last_task

        def dependencies
          i = @dependencies; @dependencies = []; i
        end

        def task_dependencies
          IndiferentHash.setup(@task_dependencies || {})
        end
 
        def tasks
          IndiferentHash.setup(@tasks || {})
        end
      end

      if defined? Rbbt
        base.workdir = Rbbt.var.jobs.find
      else
        base.workdir = Path.setup('var/jobs')
      end
      base.tasks = {}
      base.dependencies = []
      base.task_dependencies = {}
      base.task_description = {}
      base.asynchronous_exports = []
      base.synchronous_exports = []
      base.exec_exports = []
      base.libdir = Path.caller_lib_dir
    end
    self.workflows << base
  end

  # {{{ Task definition helpers
  
  def task(name, &block)
    if Hash === name
      result_type = name.first.last
      name = name.first.first
    else
      result_type = :marshal
    end

    name = name.to_sym

    block = self.method(name) unless block_given?

    result_type = result_type
    task = Task.setup({
      :name => name,
      :inputs => inputs,
      :description => description,
      :input_types => input_types,
      :result_type => Array == result_type ? result_type.to_sym : result_type,
      :input_defaults => input_defaults,
      :input_descriptions => input_descriptions
    }, &block)

    @last_task = task
    @tasks[name] = task
    @task_dependencies[name] = dependencies
  end

  def export_exec(*names)
    @exec_exports.concat names
  end

  def export_asynchronous(*names)
    @asynchronous_exports.concat names
  end

  def export_synchronous(*names)
    @synchronous_exports.concat names
  end

  # {{{ Job management
  
  def resolve_locals(inputs)
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

  def job(taskname, jobname = nil, inputs = {})
    jobname ||= "Default"
    task = tasks[taskname]
    raise "Task not found: #{ taskname }" if task.nil?


    IndiferentHash.setup(inputs)

    resolve_locals(inputs)


    dependencies = real_dependencies(task, jobname, inputs, task_dependencies[taskname] || [])

    input_values = task.take_input_values(inputs)

    step_path = step_path taskname, jobname, input_values, dependencies

    step = Step.new step_path, task, input_values, dependencies

    step
  end

  def load(path)
    task = task_for path
    Step.new path, tasks[task]
  end

  def load_id(id)
    path = File.join(workdir, id)
    task = task_for path
    step = Step.new path, tasks[task]
    if step.info.include? :dependencies
      step.dependencies = step.info[:dependencies].collect do |task, job|
        Step.new File.join(workdir, task.to_s, job), tasks[task]
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
