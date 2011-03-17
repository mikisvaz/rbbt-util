#!/usr/bin/env ruby
require 'spreadsheet'
require 'rbbt/util/simpleopt'

options = SOPT.get("-h--header:-o--outfile*:-i--infile*:-p--page*")

infile  = options[:infile] || "-"
outfile = options[:outfile] || "-"

page    = options[:page]   || 1
page = page.to_i - 1

i = (infile == '-' ? STDIN : File.open(infile))

workbook = Spreadsheet.open i
sheet    = workbook.worksheet 0

rows = []

sheet.each do |row|
  rows << row.values_at(0..(row.size - 1))
end

if options[:header]
  header = rows.shift
  puts "#" + header * "\t"
end

rows.each do |row| puts row * "\t" end
