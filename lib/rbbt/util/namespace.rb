require 'rbbt/util/misc'
module NameSpace

  ALIAS = {}

  def self.alias(namespace, real)
    namespace = namespace.name if namespace.respond_to? :name
    real = namespace.name if real.respond_to? :name
    ALIAS[namespace] = real
  end

  def self.namespace(string)
    string.extend NameSpace
  end

  def to_mod
    tmp_namespace = self.to_s
    tmp_namespace = ALIAS[tmp_namespace] if ALIAS.include? tmp_namespace
    Misc.string2const(tmp_namespace)
  end

  def ==(other)
    return true if other.nil?
    this_namespace = self.to_s
    this_namespace = ALIAS[this_namespace] if ALIAS.include? this_namespace
    other_namespace = other.to_s
    other_namespace = ALIAS[other_namespace] if ALIAS.include? other_namespace

    this_namespace == other_namespace
  end

  def identifier_files
    mod = to_mod
    begin
      identifier_files = []
      while true
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
