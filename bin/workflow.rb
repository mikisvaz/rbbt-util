#!/usr/bin/env ruby

require 'rbbt-util'
require 'rbbt/util/simpleopt'
require 'rbbt/util/workflow'

options = SOPT.get "-h--help:-t--target*:-n--name*"

raise "No target" unless options[:target]

file    = ARGV.shift

if ARGV.empty?
  data = STDIN.read
else
  data = ARGV.shift
end

job = options[:name] || "Job"

puts "Runing WorkFlow in #{file} for target #{options[:target]}. Job: #{job}"
WorkFlow.load file, File.join(options[:target], job), data
puts 
puts "WorkFlow done. Please find results in: #{File.join(options[:target], job)}"
