#!/usr/bin/env ruby

require 'rbbt-util'
require 'rbbt/util/simpleopt'

$0 = "rbbt #{$previous_commands*" "} #{ File.basename(__FILE__) }" if $previous_commands

options = SOPT.setup <<EOF

Find a path

$ #{$0} [options] [<subpath>] <path>

Use - to read from STDIN

-h--help Print this help
-w--workflows Workflow to load
-s--search_path* Workflow to load
-l--list List contents of resolved directories
-n--nocolor Don't color output
EOF
if options[:help]
  if defined? rbbt_usage
    rbbt_usage 
  else
    puts SOPT.doc
  end
  exit 0
end

subpath, path = ARGV
path, subpath = subpath, nil if path.nil?

begin
  require 'rbbt/workflow'
  workflow = Workflow.require_workflow subpath
  subpath = workflow.libdir
rescue
  Log.exception $!
end if subpath && subpath =~ /^[A-Z][a-zA-Z]+$/

path = subpath ? Path.setup(subpath)[path] : Path.setup(path)

search_path = options[:search_path].to_sym if options.include? :search_path
nocolor = options[:nocolor]

found = if search_path
          [path.find(search_path)]
        else
          path.find_all
        end

found.each do |path|
  if options[:list] && File.directory?(path)
    puts Log.color :blue, path
    path.glob("*").each do |subpath|
      if nocolor
        puts subpath
      else
        color = File.directory?(subpath) ? :blue : nil
        puts " " << Log.color(color, subpath)
      end
    end
  else
    if nocolor
      puts path
    else
      color = File.exists?(path) ? (File.directory?(path) ? :blue : nil) : :red
      puts Log.color color, path
    end

  end
end

