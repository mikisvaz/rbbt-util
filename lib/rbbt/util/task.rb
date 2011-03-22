require 'rbbt/util/open'
require 'rbbt/util/task/job'

class Task
  class << self
    attr_accessor :basedir
  end

  @basedir = "."

  def load(job_id)
    Job.load(self, job_id)
  end

  def job_id(name, job_options, previous_jobs)
    job_options = job_options.merge :previous_jobs => previous_jobs.collect{|job| job.id} if previous_jobs.any?
    if job_options.any?
      name.to_s + "_" + Misc.hash2md5(job_options)
    else
      name.to_s
    end
  end

  attr_accessor :name, :persistence, :options, :option_descriptions, :option_types, :option_defaults, :workflow, :dependencies, :scope, :description, :block
  def initialize(name, persistence = nil, options = nil, option_descriptions = nil, option_types = nil, option_defaults = nil, workflow = nil, dependencies = nil, scope = nil, description = nil, &block)
    dependencies = [dependencies] unless dependencies.nil? or Array === dependencies
    @name = name.to_s

    @persistence = persistence || :marshal

    @options = Array === options ? options : [options] unless options.nil? 

    @option_defaults = option_defaults 
    @option_descriptions = option_descriptions 
    @option_types = option_types 
    @workflow = workflow
    @dependencies = dependencies || []
    @scope = scope
    @description = description

    @block = block unless not block_given?
  end

  def process_options(args, optional_args)
    run_options = {}

    options.each do |option|
      if option_defaults and option_defaults.include? option
        run_options[option] = Misc.process_options(optional_args, option) || option_defaults[option]
      else
        run_options[option] = args.shift
      end
    end unless options.nil?
 
    [run_options, args, optional_args]
  end

  def setup(jobname, args, optional_args, dependencies)
    previous_jobs = []
    required_files = []
  
    run_options, args, optional_args = process_options args, optional_args

    dependencies.each do |dependency|
      case
      when Proc === dependency
        previous_jobs << dependency.call(jobname, run_options)
      when Task === dependency
        previous_jobs << dependency.job(jobname, *(args + [optional_args]))
      when Task::Job === dependency
        previous_jobs << dependency
      when Symbol === dependency
        previous_jobs << workflow.tasks[dependency].job(jobname, *(args + [optional_args]))
      else
        required_files << dependency
      end
    end

    [previous_jobs, required_files, run_options]
  end

  def job(jobname, *args)
    if Hash === args.last
      optional_args = args.pop
    else
      optional_args = {}
    end

    previous_jobs, required_files, run_options = setup(jobname, args, optional_args, dependencies)

    job_id = self.job_id jobname, run_options, previous_jobs

    Job.new(self, job_id, jobname, run_options, previous_jobs, required_files, previous_jobs.first)
  end

  def run(*args)
    job(*args).start
  end

end
