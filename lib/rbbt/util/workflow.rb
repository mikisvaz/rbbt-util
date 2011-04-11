require 'rbbt/util/resource'
require 'rbbt/util/task'
require 'rbbt/util/persistence'
require 'rbbt/util/misc'

module WorkFlow
  def self.extended(base)
    class << base
      attr_accessor :tasks, :jobdir, :dangling_options, :dangling_option_descriptions,
        :dangling_option_types, :dangling_option_defaults, :dangling_dependencies, :last_task
    end

    base.extend Resource
    base.lib_dir = Resource.caller_base_dir if base.class == Object
    base.tasks = {}
    base.tasks.extend IndiferentHash
    base.jobdir = (File.exists?(base.var.find(:lib)) ? base.var.find(:lib) : base.var.find)
    base.clear_dangling
  end

  def tasks=(tasks)
    tasks.extend IndiferentHash
    @tasks = tasks
  end

  def local_persist(*args, &block)
    argsv = *args
    options = argsv.pop
    if Hash === options
      options.merge!(:persistence_dir => cache.find(:lib))
      argsv.push options
    else
      argsv.push options
      argsv.push({:persistence_dir => cache.find(:lib)})
    end
    Persistence.persist(*argsv, &block)
  end

  def clear_dangling
    @dangling_options = []
    @dangling_option_descriptions = {}
    @dangling_option_types = {} 
    @dangling_option_defaults = {}
    @dangling_dependencies = nil
    @dangling_description = nil
  end

  def task_option(*args)
    name, description, type, default = *args
    @dangling_options << name if name
    @dangling_option_descriptions[name] = description if description
    @dangling_option_types[name] = type if type
    @dangling_option_defaults[name] = default if default
  end

  def task_dependencies(dependencies)
    dependencies = [dependencies] unless Array === dependencies
    @dangling_dependencies = dependencies.collect{|dep| Symbol === dep ? tasks[dep] : dep }
  end

  def task_description(description)
    @dangling_description = description
  end

  def process_dangling
    res = [ 
      @dangling_options, 
      Hash[*@dangling_options.zip(@dangling_option_descriptions.values_at(*@dangling_options)).flatten],
      Hash[*@dangling_options.zip(@dangling_option_types.values_at(*@dangling_options)).flatten],
      Hash[*@dangling_options.zip(@dangling_option_defaults.values_at(*@dangling_options)).flatten],
      (@dangling_dependencies || [@last_task]).compact,
      @dangling_description,
    ]

    clear_dangling
    res
  end

  def task(name, &block)
    if Hash === name
      persistence = name.values.first
      name        = name.keys.first
    else
      persistence = :marshal
    end

    options, option_descriptions, option_types, option_defaults, dependencies, description = process_dangling
    option_descriptions.delete_if do |k,v| v.nil? end
    option_types.delete_if do |k,v| v.nil? end
    option_defaults.delete_if do |k,v| v.nil? end
    task = Task.new name, persistence, options, option_descriptions, option_types, option_defaults, self, dependencies, self, description, &block
    tasks[name] = task
    @last_task = task
  end

  def job(task, jobname, *args)
    tasks[task].job(jobname, *args)
  end

  def run(*args)
    job(*args).run
  end

  def load_job(taskname, job_id)
    tasks[taskname].load(job_id)
  end

end
