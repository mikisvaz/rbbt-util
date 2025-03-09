#!/usr/bin/env ruby

code_file = ARGV[0]
output = ARGV[1]

require 'rbbt-util'

code = case 
       when (code_file.nil? or code_file == '-')
         STDIN.read
       else
         Open.read(code_file)
       end

begin
  if code_file.nil?
    data = instance_eval code
  else
    data = instance_eval code, code_file
  end
rescue Exception
  puts "#:rbbt_exec Error"
  puts $!.message
  puts $!.backtrace * "\n"
  exit(-1)
end

#data = data.to_s(:sort) if TSV === data
data = data * "\n" if Array === data

case
when (output.nil? or output == '-')
  STDOUT.write data
when output == "file"
  if Misc.filename? data
    tmpfile = data
  else
    tmpfile = TmpFile.tmp_file
    Open.write(tmpfile, data.to_s)
  end

  STDOUT.puts tmpfile
else
  Open.write(output, data.to_s)
end
