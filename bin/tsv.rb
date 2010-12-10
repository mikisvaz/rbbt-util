#!/usr/bin/env ruby

require 'rbbt/util/simpleopt'

options = SOPT.parse "-h--help:-to--tsv-options*:-p--persistence"

command = ARGV.shift
file    = ARGV.shift

case command
when 'cat'
  puts TSV.new(file, options["tsv-options"].merge(options["persistence"]))
when '

