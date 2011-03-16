require 'rbbt/util/open'

class Task
  class << self
    attr_accessor :basedir
  end

  @basedir = "."

  class Job
    attr_accessor :task, :id, :name, :options, :dependencies, :pid, :path, :previous_jobs

    def initialize(task, id, name, options, dependencies)
      @task = task  
      @id =id
      @name = name
      @options = options
      @dependencies = dependencies

      @previous_jobs = Hash[*dependencies.first.collect{|job| job.task.name}.zip(dependencies.first).flatten]
      dependencies.first.collect{|job| @previous_jobs.merge! job.previous_jobs }

      basedir = task.workflow.basedir unless task.workflow.nil?
      @path = File.join(basedir || Task.basedir, task.name, id)
    end

    def info_file
      path + '.info'
    end

    def info
      return {} if not File.exists?(info_file)
      YAML.load(File.open(info_file))
    end

    def set_info(key, value)
      i = self.info
      new_info = i.merge(key => value)
      Open.write(info_file, new_info.to_yaml)
    end

    def step(name = nil, message = nil)
      if name.nil?
        info[:step]
      else
        set_info(:step, name)
        set_info(:messages, info[:messages] || [] << message) if not message.nil?
      end
    end
    
    def messages
      info[:messages] || []
    end

    def done?
      [:done, :error, :aborted].include? info[:step]
    end

    def error?
      step == :error or step == :aborted
    end

    def arguments
      options.values_at *task.options
    end

    def block
      task.block
    end

    def run_dependencies
      jobs, files = dependencies
      jobs.each do |job| job.start unless File.exists? job.path end
      files.each do |file| file.produce end
    end

    def input
      dep = dependencies.flatten.first
      if Job === dep
        dep.load
      else
        dep.read
      end
    end

    def start
      Log.medium("Starting Job '#{ name }'. Path: '#{ path }'")
      set_info(:start_time, Time.now)

      if dependencies.flatten.any?
        run_dependencies
      end

      result = instance_exec *arguments, &block

      if not result.nil?
        case task.persistence
        when nil, :string, :tsv, :integer
          Open.write(path, result.to_s)
        when :marshal
          Open.write(path, Marshal.dump(result))
        when :yaml
          Open.write(path, YAML.dump(result))
        end
      end

      set_info(:end_time, Time.now)
      Log.medium("Finished Job '#{ name }'. Path: '#{ path }'")
    end

    def save_options(options)
      new_options = {}
      options.each do |key, value|
        case 
        when TSV === value
         new_options[key] = value.to_s
        else
          new_options[key] = value
        end
      end
      set_info(:options, new_options)
    end

    def run
      begin
        step(:started)
        start
        step(:done)
      rescue Exception
        Log.debug $!.message
        Log.debug $!.backtrace * "\n"
        step(:error, "#{$!.class}: #{$!.message}")
      end
    end

    def fork
      @pid = Process.fork do
        begin
          step(:started)
          save_options(options)
          start
          step(:done)
        rescue Exception
          Log.debug $!.message
          Log.debug $!.backtrace * "\n"
          step(:error, "#{$!.class}: #{$!.message}")
        end
        exit
      end

      self
    end

    def join
      if @pid.nil?
        while info[:step] != :done do
          sleep 5
        end
      else
        Process.waitpid @pid
      end

      self
    end

    def open
      File.open(path)
    end

    def read
      File.open(path) do |f| f.read end
    end

    def load
      case task.persistence
      when :float
        Open.read(path).to_f
      when :integer
        Open.read(path).to_i
      when :string
        Open.read(path)
      when :tsv
        TSV.new(path)
      when :marshal
        Marshal.load(Open.read(path))
      when :yaml
        YAML.load(Open.read(path))
      end
    end

    def clean
      FileUtils.rm path if File.exists? path
      FileUtils.rm info_file if File.exists? info_file
    end

    def recursive_clean
      dependencies.first.each do |job| job.recursive_clean end
      clean
    end
  end # END Job

  def job_options(run_options = nil)
    return {} if options.nil?

    job_options = {}
    options.each do |option|
      job_options[option] = Misc.process_options run_options, option
    end

    job_options
  end

  def job_id(name, job_options)
    if job_options.any?
      name.to_s + "_" + Misc.hash2md5(job_options)
    else
      name.to_s
    end
  end

  attr_accessor :name, :persistence, :options, :option_descriptions, :option_types, :option_defaults, :workflow, :dependencies, :block
  def initialize(name, persistence = nil, options = nil, option_descriptions = nil, option_types = nil, option_defaults = nil, workflow = nil, dependencies = nil, &block)
    dependencies = [dependencies] unless dependencies.nil? or Array === dependencies
    @name = name.to_s

    @persistence = persistence || :marshal

    @options = Array === options ? options : [options] unless options.nil? 

    @option_defaults = option_defaults 
    @option_descriptions = option_descriptions 
    @option_types = option_types 
    @workflow = workflow
    @dependencies = dependencies || []

    @block = block unless not block_given?
  end

  def recursive_options
    all_options         = []
    option_descriptions = {}
    option_types        = {}
    option_defaults     = {}

		all_options.concat           self.options               if   self.options
		option_descriptions.merge!   self.option_descriptions   if   self.option_descriptions
		option_types.merge!          self.option_types          if   self.option_types
		option_defaults.merge!       self.option_defaults       if   self.option_defaults

    self.dependencies.each do |task|
      task = case
             when Symbol === task
               workflow.tasks[task]
             when Task === task
               task
             else
               next
             end

      n_all_options, n_option_descriptions, n_option_types, n_option_defaults = task.recursive_options

			all_options.concat           n_all_options           if   n_all_options
			option_descriptions.merge!   n_option_descriptions   if   n_option_descriptions
			option_types.merge!          n_option_types          if   n_option_types
			option_defaults.merge!       n_option_defaults       if   n_option_defaults
		end

    [all_options, option_descriptions, option_types, option_defaults]
  end

  def job_dependencies(jobname, run_options = {})
    jobs = []
    files = []
    dependencies.each do |dependency|
      case
      when Task === dependency
        jobs << dependency.job(jobname, run_options)
      when Symbol === dependency
        raise "No workflow defined, yet dependencies include Symbols (other tasks)" if workflow.nil?
        jobs << workflow.tasks[dependency].job(jobname, run_options)
      else
        files << dependency
      end
    end
    [jobs, files]
  end

  def job(jobname, run_options = {})

    job_id = self.job_id jobname, run_options

    job_options = self.job_options run_options

    dependencies = self.job_dependencies(jobname, run_options) 

    Job.new(self, job_id, jobname, job_options, dependencies)
  end

  def run(*args)
    job(*args).start
  end

end
