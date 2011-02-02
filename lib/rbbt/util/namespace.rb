require 'rbbt/util/misc'
module NameSpace

  def self.namespace(string)
    string.extend NameSpace
  end

  def identifier_files
    tmp_namespace = self
    begin
      identifier_files = []
      while true
        mod = Misc.string2const(tmp_namespace)
        identifier_files << mod.identifiers if mod.identifiers.exists?
        if tmp_namespace =~ /::/
          tmp_namespace = tmp_namespace.sub(/(.*)::.*/,'\1')
          tmp_namespace.extend NameSpace
        else
          break
        end
      end
      raise if identifier_files.empty?
      identifier_files.collect{|f| Path.path(f, Misc.string2const(self).datadir, self)}
    rescue
      Log.debug "No identifier files in #{ self } namespace"
      nil
    end
  end
end
