#!/usr/bin/env ruby

require 'rbbt-util'
require 'rbbt/util/simpleopt'
require 'rbbt/util/workflow'
require 'pp'

def usage(task)
  puts task.usage
  exit -1
end

def SOPT_options(task)
  sopt_options = []
  task.option_summary.flatten.each do |info|
    name = info[:name]
    short = name.to_s.chars.first
    boolean = info[:type] == :boolean
    
    sopt_options << "-#{short}--#{name}#{boolean ? '' : '*'}"
  end

  sopt_options * ":"
end

def fix_options(task, job_options)
  option_types = task.option_summary.flatten.inject({}){|types, new| types[new[:name]] = new[:type]; types}

  job_options_cleaned = {}

  job_options.each do |name, value|
    value = case
            when option_types[name] == :float
              value.to_f
            when option_types[name] == :integer
              value.to_i
            when option_types[name] == :tsv
              begin
                if value == '-'
                  TSV.new(STDIN).to_s :sort
                else
                  TSV.new(value).to_s :sort
                end
              rescue
                value
              end
            else
              value
            end
    job_options_cleaned[name] = value
  end

  job_options_cleaned
end

options = SOPT.get "-t--task*:-l--log*:-h--help:-n--name:-cl--clean:-rcl-recursive_clean"

# Set log, fork, clean, recursive_clean and help
Log.severity = options[:log].to_i if options.include? :log
help = !!options.delete(:help)
do_fork = !!options.delete(:fork)
clean = !!options.delete(:clean)
recursive_clean = !!options.delete(:recursive_clean)

# Get workflow
workflow = ARGV.first
WorkFlow.require_workflow workflow

# Set task
namespace, task = nil, nil

case 
when (not options[:task])
  workflow_usage if help
  task = self.last_task
  namespace = self
when (options[:task] =~ /\./)
  namespace, task = options.delete(:task).split('.')
  namespace = Misc.string2const(namespace)
else
  task_name = options.delete(:task)
  self.tasks[task_name]
end

usage(task) if help

name = options.delete(:name) || "Default"

# get job args
sopt_option_string = SOPT_options(task)
job_options = SOPT.get sopt_option_string
job_options = fix_options(task, job_options)

#- get job
job = task.job(name, job_options)

# clean job
job.clean if clean
job.recursive_clean if recursive_clean

# run
if do_fork
  job.fork
  while not job.done?
    puts "#{job.step}: #{job.messages.last}"
    sleep 2
  end
else
  job.run
end

#- error
raise job.messages.last if job.error?

#print
pp job.load
