require 'rbbt/util/open'
require 'rbbt/util/misc'

lockfiles= `find . -name *.lock`.split "\n"
lockfiles.each do |file|
  Open.read(file) =~ /^pid: (\d+)/
  pid = $1
  puts [file + " (#{ pid })", Misc.pid_exists?(pid) ? "Running" : "Missing"] * ": "
end
