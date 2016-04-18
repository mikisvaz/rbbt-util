#{{{ Require files
Rbbt.etc.requires.read.split("\n").each do |file|
  next if file.empty?
  Log.debug("requiring #{ file }")
  require file
end if Rbbt.etc.requires.exists?

