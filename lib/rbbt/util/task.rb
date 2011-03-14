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
      return {} if not File.exists? info_file
      YAML.load(File.open(info_file))
    end

    def set_info(key, value)
      Open.write info_file, YAML.dump(info.merge(key => value))
    end

    def step(name)
      set_info(:step, name)
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
      Log.medium("Starting Job '#{ name }'. Path: '#{ path }'. Options #{options.inspect}")

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
    end

    def fork
      @pid = Process.fork do
        step(:started)
        set_info(:options, options)
        start
        step(:done)
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
  end

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

    @persistence = persistence || :string

    @options = Array === options ? options : [options] unless options.nil? 

    @option_defaults = option_defaults 
    @option_descriptions = option_descriptions 
    @option_types = option_types 
    @workflow = workflow
    @dependencies = dependencies || []

    @block = block unless not block_given?
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
