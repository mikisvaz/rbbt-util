require 'rbbt/util/task'
module WorkFlow
  def self.extended(base)
    class << base
      attr_accessor :tasks, :basedir, :dangling_options, :dangling_option_descriptions,
        :dangling_option_types, :dangling_option_defaults, :dangling_dependencies, :last_task
    end

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
      @dangling_option_descriptions.values_at(*@dangling_options),
      @dangling_option_types.values_at(*@dangling_options),
      @dangling_option_defaults.values_at(*@dangling_options),
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
    task = Task.new name, persistence, options, option_descriptions, option_types, option_defaults, self, dependencies, &block
    tasks[name] = task
    @last_task = task
  end

end
