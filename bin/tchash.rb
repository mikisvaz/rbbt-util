#!/usr/bin/env ruby

require 'rbbt-util'
require 'rbbt/util/simpleopt'

options = SOPT.parse "-h--help"

command = ARGV.shift
file    = ARGV.shift

case command
when 'cat'
  puts TSV.new(TCHash.get(file))
end

