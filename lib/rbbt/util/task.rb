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

  def pull_from_hash(args, optional_args)
    option_summary.first.each do |info|
      name = info[:name]
      if optional_args.include? name
        args.push optional_args.delete name
      end
    end
  end

  def setup(jobname, args, optional_args, dependencies)
    previous_jobs = []
    required_files = []
  
    pull_from_hash(args, optional_args)
    run_options, args, optional_args = process_options args, optional_args

    dependencies.each do |dependency|
      case
      when Proc === dependency
        deps = dependency.call(jobname, run_options)
        if Array === deps
          previous_jobs.concat deps
        else
          previous_jobs << deps
        end
      when Task::Job === dependency
        previous_jobs << dependency
      when Task === dependency
        previous_jobs << dependency.job(jobname, *(args + [optional_args]))
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

  def option_info(option)
    info = {}
    info[:name] = option
    info[:source] = name
    info[:description] = option_descriptions[option] if option_descriptions and option_descriptions.include? option
    info[:type] = option_types[option] if option_types and option_types.include? option
    info[:default] = option_defaults[option] if option_defaults and option_defaults.include? option

    info
  end

  def option_summary
    options = []
    optional_options = []

    if self.options
      self.options.collect{|option|
        info = option_info(option)
        if info[:default].nil?
          options << info
        else
          optional_options << info
        end
      }
    end

    dependencies.select{|dep| Task === dep}.each do |task|
      more_options, more_optional_options = task.option_summary
      options.concat more_options
      optional_options.concat more_optional_options
    end

    [options, optional_options]
  end

  def usage
    usage = ""
    usage << "Task: #{name}\n"
    usage << "\nDescription: #{description.chomp}\n" if description
    options, optional_options = option_summary

    if options.any?
      usage << "\nMandatory options:\n"
      usage << "\tTask\tName\tType   \tDescription\n"
      usage << "\t----\t----\t----   \t-----------\n"

      options.each do |option|
        option_line = "\t[#{option[:source]}]\t#{option[:name]}"
        option_line << "\t#{option[:type] ? option[:type] : "Unspec."}"
        option_line << "\t#{option[:description]}" if option[:description]
        usage << option_line << "\n"
      end
    end

    if optional_options.any?
      usage << "\nOptional options:\n"
      usage << "\tTask\tName\tDefault  \tType   \tDescription\n"
      usage << "\t----\t----\t-------  \t----   \t-----------\n"
      optional_options.each do |option|
        option_line = "\t[#{option[:source]}]\t#{option[:name]}\t#{option[:default]}"
        option_line << "\t#{option[:type] ? option[:type] : "Unspec."}"
        option_line << "\t#{option[:description]}" if option[:description]
        usage << option_line << "\n"
      end
    end

    usage
  end

end
