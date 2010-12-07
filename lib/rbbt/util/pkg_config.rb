require 'yaml'

module PKGConfig
  class NoConfig < Exception; end
  class NoVariables < Exception; end

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
