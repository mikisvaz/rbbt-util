#!/usr/bin/env ruby

require 'rbbt-util'
require 'fileutils'
require 'rbbt/util/simpleopt'
require 'rbbt/workflow/step'

options = SOPT.get("-l--list:-z--zombies:-e--errors:-c--clean:-n--name:-a--all:-w--wipe:-f--file*")

def info_files
  Dir.glob('**/*.info')
end

def running?(info)
  begin
    Process.kill 0, info[:pid]
    true
  rescue
    false
  end
end

def print_job(file, info)
  clean_file = file.sub('.info','')
  if $name
    puts clean_file
  else
    str = [clean_file, info[:status].to_s] * " [ STATUS = " + " ]" 
    str += " (#{running?(info)? :running : :zombie})" if info.include? :pid
    puts str
  end
end

def list_jobs(options)
  omit_ok = options[:zombies] || options[:errors]
  info_files.each do |file|
    clean_file = file.sub('.info','')
    next if File.exists? clean_file
    info = YAML.load(Open.read(file))
    case
    when (not omit_ok)
      print_job file, info
    when options[:zombies]
      print_job file, info if info[:pid] and not running? info
    when options[:errors]
      print_job file, info if info[:status] == :error
    end

  end
end

def remove_job(file)
  clean_file = file.sub('.info','')
  FileUtils.rm file if File.exists? file
  FileUtils.rm clean_file if File.exists? clean_file
  FileUtils.rm_rf clean_file + '.files' if File.exists? clean_file + '.files'
end

def clean_jobs(options)
  info_files.each do |file|
    clean_file = file.sub('.info','')
    next if File.exists? clean_file
    info = YAML.load(Open.read(file))
    case
    when options[:all]
      remove_job file
    when (options[:zombies] and info[:pid] and not running? info)
      remove_job file
    when (options[:errors] and info[:status] == :error)
      remove_job file
    end

  end
end



$name = options.delete :name
case
when (options[:clean] and not options[:list])
  if options[:file]
    remove_job options[:file]
  else
    clean_jobs options
  end
else
  if options[:file]
    info = YAML.load(Open.read(options[:file]))
    print_job options[:file], info
  else
    list_jobs options
  end
end
