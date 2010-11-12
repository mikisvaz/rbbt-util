require 'yaml'
module PKGConfig
  class NoConfig < Exception; end
  class NoVariables < Exception; end

  def self.load_config(target_class, file, pkg_variables)
    config = YAML.load_file(file)

    pkg_variables.each do |variable|
      target_class.send("#{variable}=", config[variable])
    end
  end

  def self.load_cfg(target_class, pkg_variables)

    pkg_cfg_files = [File.join(ENV["HOME"], '.' + target_class.to_s)]

    pkg_variables.each do |variable|
      target_class.class_eval %{
        def self.#{variable} 
          @#{variable}
        end
        def self.#{variable}=(value)
          @#{variable} = value
        end
      }
    end

    self.methods.sort

    pkg_cfg_files.each do |file|
      if File.exists? file
        load_config target_class, file, pkg_variables
        break
      end
      raise NoConfig, "No config file found. [#{pkg_cfg_files * ", "}]"
    end
  end

  def self.included(base)
    raise NoVariables, "No CFG_VARIABLES array was initialized" unless  defined?(base::CFG_VARIABLES)
    load_cfg(base, base::CFG_VARIABLES)
  end
end
