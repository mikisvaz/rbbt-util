require 'yaml'

module PKGConfig
  class NoConfig < Exception; end
  class NoVariables < Exception; end

  class RootdirNotFoundError < StandardError; end

  def self.rootdir_for_file(file = __FILE__)
    dir = File.expand_path(File.dirname file)

    while not File.exists?(File.join(dir, 'lib')) and dir != '/'
      dir = File.dirname(dir)
    end

    if File.exists? File.join(dir, 'lib')
      dir
    else
      raise RootdirNotFoundError
    end
  end

  def self.get_caller_rootdir
    caller.each do |line|
      next if line =~ /\/pkg_config\.rb/ 
        begin
          return PKGConfig.rootdir_for_file(line)
        rescue RootdirNotFoundError
        end
    end
    raise RootdirNotFoundError
  end


  def self.extended(base)
    base.module_eval{ @@rootdir = PKGConfig.get_caller_rootdir}
  end

  def rootdir
    @@rootdir
  end

  def load_config(file, pkg_variables)
    config = YAML.load_file(file)

    pkg_variables.each do |variable|
      self.send("#{variable}=", config[variable])
    end
  end

  def load_cfg(pkg_variables, default = nil)
    pkg_cfg_files = [ ENV['RBBT_CONFIG'] || "", 
      File.join(ENV["HOME"], '.' + self.to_s), 
      File.join('/etc/', '.' +  self.to_s)]

    pkg_variables.each do |variable|
      self.class_eval %{
        def self.#{variable} 
          @#{variable}
        end
        def self.#{variable}=(value)
          @#{variable} = value
        end
      }
    end

    file = pkg_cfg_files.select{|file| File.exists? file}.first
    if file.nil?
      if default
        file = pkg_cfg_files[1]
        Open.write(file, default)
      else
        raise NoConfig, "No config file found. [#{pkg_cfg_files * ", "}]" if file.nil?
      end
    end
    load_config file, pkg_variables
  end
end
