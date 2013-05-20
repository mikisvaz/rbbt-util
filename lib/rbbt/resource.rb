require 'rbbt/util/open'
require 'rbbt/util/log'
require 'rbbt/util/chain_methods'
require 'rbbt/resource/path'
 
module Resource
  extend ChainMethods
  self.chain_prefix = :resource

  def self.extended(base)
    setup_chains(base)
    if not base.respond_to? :pkgdir
      class << base
        attr_accessor :pkgdir, :subdir, :resources, :rake_dirs
      end

      base.pkgdir = 'rbbt'
      base.subdir = ''
      base.resources = {}
      base.rake_dirs = {}
    end
    base
  end

  def root()
    Path.setup @subdir || "", @pkgdir, self
  end

  def resource_method_missing(name, prev = nil, *args)
    # Fix problem with ruby 1.9 calling methods by its own initiative. ARG
    if prev.nil?
      root.send(name, *args)
    else
      root.send(name, prev, *args)
    end
  end

  def [](file = nil)
    if file.nil?
      root
    else
      root.send(file)
    end
  end

  def claim(path, type, content = nil, &block)
    if type == :rake
      @rake_dirs[path] = content
    else
      @resources[path] = [type, content || block]

      if type == :install
        Log.debug "Preparing software: #{path}"
        path.produce
        software_dir = path.resource.root.software
        set_software_env(software_dir)
      end
    end
  end

  def produce(path, force = false)
    case
    when @resources.include?(path)
      type, content = @resources[path]
    when has_rake(path)
      type = :rake
      rake_dir, content = rake_for(path)
      rake_dir = Path.setup(rake_dir.dup, self.pkgdir, self)
    else
      raise "Resource #{ path } does not seem to be claimed"
    end

    final_path = path.respond_to?(:find) ? (force ? path.find(:user) : path.find) : path
    if not File.exists? final_path or force
      Misc.lock final_path + '.produce' do
        begin
          case type
          when :string
            Open.write(final_path, content)
          when :url
            Open.write(final_path, Open.open(content))
          when :proc
            data = case content.arity
                   when 0
                     content.call
                   when 1
                     content.call final_path
                   end
            Open.write(final_path, data) unless data.nil?
          when :rake
            run_rake(path, content, rake_dir)
          when :install
            Log.debug "Installing software: #{path}"
            software_dir = path.resource.root.software.find :user
            preamble = <<-EOF
#!/bin/bash

RBBT_SOFTWARE_DIR="#{software_dir}"

INSTALL_HELPER_FILE="#{Rbbt.share.install.software.lib.install_helpers.find :lib, caller_lib_dir(__FILE__)}"
source "$INSTALL_HELPER_FILE"
            EOF

            CMD.cmd('bash', :in => preamble + "\n" + Open.read(content))

            set_software_env(software_dir)
          else
            raise "Could not produce #{ resource }. (#{ type }, #{ content })"
          end
        rescue
          FileUtils.rm_rf final_path if File.exists? final_path
          raise $!
        end
      end
    end

    path
  end
end
