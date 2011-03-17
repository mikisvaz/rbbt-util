require 'rbbt/util/task'
module WorkFlow
  def self.extended(base)
    class << base
      attr_accessor :tasks, :basedir, :base, :dangling_options, :dangling_option_descriptions,
        :dangling_option_types, :dangling_option_defaults, :dangling_dependencies, :last_task
    end

    base.base = base
    base.tasks = {}
    base.basedir = '.'
    base.clear_dangling
  end

  def clear_dangling
    @dangling_options = []
    @dangling_option_descriptions = {}
    @dangling_option_types = {} 
    @dangling_option_defaults = {}
    @dangling_dependencies = nil
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
    @dangling_dependencies = dependencies
  end

  def process_dangling
    res = [ 
      @dangling_options, 
      Hash[*@dangling_options.zip(@dangling_option_descriptions.values_at(*@dangling_options)).flatten],
      Hash[*@dangling_options.zip(@dangling_option_types.values_at(*@dangling_options)).flatten],
      Hash[*@dangling_options.zip(@dangling_option_defaults.values_at(*@dangling_options)).flatten],
      @dangling_dependencies || @last_task,
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

    options, option_descriptions, option_types, option_defaults, dependencies = process_dangling
    option_descriptions.delete_if do |k,v| v.nil? end
    option_types.delete_if do |k,v| v.nil? end
    option_defaults.delete_if do |k,v| v.nil? end
    task = Task.new name, persistence, options, option_descriptions, option_types, option_defaults, self, dependencies, self, &block
    tasks[name] = task
    @last_task = task
  end

  def job(task, jobname, *args)
    task = tasks[task]
    raise "Task #{ task } not found" if task.nil?

    all_options, option_descriptions, option_types, option_defaults = task.recursive_options

    non_optional_arguments = all_options.reject{|option| option_defaults.include? option}
    run_options = nil

    case
    when args.length == non_optional_arguments.length
      run_options = Hash[*non_optional_arguments.zip(args).flatten].merge option_defaults
    when args.length == non_optional_arguments.length + 1
      optional_args = args.pop
      run_options = option_defaults.
        merge(optional_args).
        merge(Hash[*non_optional_arguments.zip(args).flatten])
    else
      raise "Number of non optional arguments (#{non_optional_arguments * ', '}) does not match given (#{args.flatten * ", "})"
    end

    task.job(jobname,  run_options)
  end

end
