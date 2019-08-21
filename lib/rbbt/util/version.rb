module Rbbt
  VERSIONS = IndiferentHash.setup({})

  def self.add_version(file)
    dir = Path.setup(Path.caller_lib_dir(file))
    libname = File.basename(dir).sub('rbbt-','')
    return if VERSIONS.include? libname

    version = if dir.VERSION.exists?
      dir.VERSION.read
    elsif dir[".git"].exists?
      begin
        head = dir[".git"]["HEAD"].read.split(" ").last.strip
        dir[".git"][head].read.strip
      rescue
        nil
      end
    elsif libname.include?("-")
      name,_sep, v = libname.partition("-")
      if v =~ /^\d+\.\d+\.\d+$/
        libname = name
        v
      else
        nil
      end
    else
      nil
    end
    return if version.nil?

    VERSIONS[libname] = version
  end

  def self.versions
    versions = Rbbt::VERSIONS
    Gem.loaded_specs.keys.each do |gem|
      next unless gem.include? 'rbbt'
      name = gem.sub('rbbt-','')
      next if versions.include? name
      version =  Gem.loaded_specs[gem].version.version
      versions[name] = version
    end
    versions
  end

  Rbbt.add_version(__FILE__)
end

