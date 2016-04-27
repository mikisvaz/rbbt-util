#{{{ Require files
Rbbt.etc.requires.read.split("\n").each do |file|
  next if file.empty?
  Log.debug("requiring #{ file }")
  begin
    require file
  rescue Exception
    Log.warn "Could not require #{ file }"
  end
end if Rbbt.etc.requires.exists?

