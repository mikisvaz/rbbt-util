#!/usr/bin/env ruby

require 'rbbt'
require 'rbbt/util/simpleopt'
require 'rbbt/workflow'
require 'pp'

def usage(task)
  puts task.usage
  exit -1
end

def SOPT_options(workflow, task)
  sopt_options = []
  workflow.rec_inputs(task.name).each do |name|
    short = name.to_s.chars.first
    boolean = workflow.rec_input_types(task.name)[name].to_sym == :boolean
    
    sopt_options << "-#{short}--#{name}#{boolean ? '' : '*'}"
  end

  sopt_options * ":"
end

def fix_options(workflow, task, job_options)
  option_types = workflow.rec_input_types(task.name)

  workflow.resolve_locals(job_options)

  job_options_cleaned = {}

  job_options.each do |name, value|
    value = case option_types[name].to_sym
            when :float
              value.to_f
            when :integer
              value.to_i
            when :string, :text
              case
              when value == '-'
                STDIN.read
              when (String === value and File.exists?(value))
                Open.read(value)
              else
                value
              end
            when :array
              if Array === value
                value
              else
                case
                when value == '-'
                  STDIN.read
                when (String === value and File.exists?(value))
                  Open.read(value)
                else
                  value
                end.split(/[,|\s]/)
              end
            when :tsv
              if TSV === value
                value
              else
                begin
                  if value == '-'
                    TSV.open(STDIN).to_s :sort
                  else
                    TSV.new(value).to_s :sort
                  end
                rescue
                  value
                end
              end
            else
              value
            end
    job_options_cleaned[name] = value
  end

  job_options_cleaned
end

options = SOPT.get "-t--task*:-l--log*:-h--help:-n--name*:-cl--clean:-rcl-recursive_clean:-pn--printname:-srv--server:-p--port*"

workflow = ARGV.first

if options[:server]

  require 'rbbt/util/log'
  require 'rbbt/workflow'
  require 'rbbt/workflow/rest'
  require 'sinatra'
  require 'compass'

  if workflow
    Workflow.require_workflow workflow
    WorkflowREST.add_workflows *Workflow.workflows
  end

  WorkflowREST.setup

  Sinatra::Application.port = options[:port] || 4567
  Sinatra::Application.run = true

  if workflow and File.exists? workflow
    Sinatra::Application.views = File.join(File.dirname(workflow), 'www/views')
  end

  sinatra_file = './lib/sinatra.rb'
  if File.exists? sinatra_file
    require sinatra_file
  end

else

  # Set log, fork, clean, recursive_clean and help
  Log.severity = options[:log].to_i if options.include? :log
  help = !!options.delete(:help)
  do_fork = !!options.delete(:fork)
  clean = !!options.delete(:clean)
  recursive_clean = !!options.delete(:recursive_clean)


  # Get workflow
  
  if Rbbt.etc.remote_workflows.exists?
    remote_workflows = Rbbt.etc.remote_workflows.yaml
  else
    remote_workflows = {}
  end

  if remote_workflows.include? workflow
    require 'rbbt/workflow/rest/client'
    workflow = RbbtRestClient.new remote_workflows[workflow], workflow
  else
    Workflow.require_workflow workflow
    workflow = Workflow.workflows.last
  end

  # Set task
  namespace, task = nil, nil

  case 
  when (not options[:task])
    usage if help
    task = workflow.last_task
    namespace = workflow
  when (options[:task] =~ /\./)
    namespace, task = options.delete(:task).split('.')
    namespace = Misc.string2const(namespace)
  else
    task_name = options.delete(:task)
    task = workflow.tasks[task_name]
    raise "Task not found: #{ task_name }" if task.nil?
  end


  workflow.usage(task) if help

  name = options.delete(:name) || "Default"

  # get job args
  sopt_option_string = SOPT_options(workflow,task)
  job_options = SOPT.get sopt_option_string
  job_options = fix_options(workflow,task, job_options)

  #- get job
  job = workflow.job(task.name, name, job_options)

  # clean job
  job.clean if clean
  job.recursive_clean if recursive_clean

  # run
  if do_fork
    job.fork
    while not job.done?
      Log.debug "#{job.step}: #{job.messages.last}"
      sleep 2
    end
    raise job.messages.last if job.error?
    res = job.load
  else
    res = job.run
  end

  if options.delete(:printname)
    puts job.name
    exit
  else
    Log.low "Job name: #{job.name}"
  end

  case
  when Array === res
    puts res * "\n"
  when TSV === res
    puts res
  when Hash === res
    puts res.to_yaml
  else
    puts res
  end
end
