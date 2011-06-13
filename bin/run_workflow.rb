#!/usr/bin/env ruby

require 'rbbt-util'
require 'rbbt/util/simpleopt'
require 'rbbt/util/workflow'

options = SOPT.get "-h--help:-t--target*:-n--name*:-l--log*:-f--fork:-cl--clean:-rcl--recursive_clean"

raise "No target" unless options[:target]

if options[:target] =~ /\./
  namespace, target = options[:target].split('.')
  namespace = Misc.string2const(namespace)
else
  target = options[:target]
end


workflow    = ARGV.shift

load workflow

task = if namespace
         namespace.tasks[target]
       else
         tasks[target]
       end

def usage(task)
  puts task.usage
  exit -1
end

usage(task) if options[:help]

Log.severity = options[:log].to_i if options.include? :log

args = []
optional_args = []
arg_types = {}

task.option_summary.first.each do |arg_info|
  name = arg_info[:name]
  arg_types[name] = arg_info[:type]
  args << name
end

task.option_summary.last.each do |arg_info|
  name = args_info[:name]
  arg_types[name] = arg_info[:type]
  optional_args << name
end

job_options_str = (args + optional_args).collect{|a| 
  name = a.to_s
  str = "-#{name.chars.first}--#{name}"
  str += "*" unless arg_types[a] == :boolean
  str
} * ":"

job_options = SOPT.get(job_options_str)

job_options_cleaned = {}

job_options.each do |name, value|
  value = case
          when arg_types[name] == :float
            value.to_f
          when arg_types[name] == :integer
            value.to_i
          else
            value
          end
  job_options_cleaned[name] = value
end

job_options = job_options_cleaned


job_args = args.collect{|arg| job_options[arg]}.collect{|v|
  v == '-' ? STDIN.read : v
}

job_optional_args = Hash[*optional_args.zip(job_options.values_at(optional_args).collect{|v| v == '-' ? STDIN.read : v}).flatten]
job_optional_args.delete_if{|k,v| v.nil?}

job_args << job_optional_args

if options[:fork]
  job = task.job((options[:name] || "Default"), *job_args)
  job.clean if options[:clean]
  job.recursive_clean if options[:recursive_clean]
  job.fork

  while not job.done?
    puts "#{job.step}: #{job.messages.last}"
    sleep 2
  end

  raise job.messages.last if job.error?

  puts job.load
else
  job = task.job((options[:name] || "Default"), *job_args)
  job.clean if options[:clean]
  job.recursive_clean if options[:recursive_clean]
  job.run
  raise job.messages.last if job.error?
  puts job.load
end
