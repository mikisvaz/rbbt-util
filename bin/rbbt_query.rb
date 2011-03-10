#!/usr/bin/env ruby

require 'rbbt-util'
require 'rbbt/util/simpleopt'
require 'rbbt/sources/organism'

options = SOPT.get("-i--identifiers*:-f--format*:-o--organism*:-p--persistence:-l--log*:-r--report")

file   = ARGV[0]

if not File.exists? file
  base, path = file.match(/(.*)?\.(.*)/).values_at 1, 2
  require 'rbbt/sources/' << base.to_s.downcase
  klass = Misc.string2const base
  file = klass[path].find
end

entities = ARGV[1].dup
persistence = options[:persistence]
log = (options[:log] || 4).to_i
Log.severity = log
organism = options[:organism] || "Hsa"
identifiers = options[:identifiers] || Organism.identifiers(organism)
format = options[:format] 
report = options[:report]

if format.to_s == "key"
  File.open(file) do |f|
    format = TSV.parse_header(f).first
  end
end

if format.nil?
  f, count = TSV.new(identifiers).guess_field(entities)
  format   = f if count > 0
end

data = if entities == '-'
  TSV.new(STDIN)
  #entities = TSV.new(STDIN.read.split(/\n|\||\t/)
else
  entities = [entities]
  data = TSV.new(entities) 
  data.type = :double
  data.identifiers = identifiers
  data.key_field = format.dup unless FalseClass === format or format.nil?
  data.fields ||= []
  data
end

data.type = :double
data.identifiers = identifiers

data.attach TSV.new file, :persistence => persistence

if report
  data.each do |entity,values|
    puts "== Entity: #{ entity }"
    puts values.report
  end
else
  puts data
end
