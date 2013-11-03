require 'rbbt/util/open'
require 'rbbt/util/log'
require 'rbbt/resource/path'
 
module Resource
  def self.remote_servers
    @remote_servers = Rbbt.etc.file_servers.exists? ? Rbbt.etc.file_servers.yaml : {}
  end

  def self.extended(base)
    base.pkgdir = 'rbbt'
    base.subdir = ''
    base.resources = {}
    base.rake_dirs = {}
    base.remote_server = Resource.remote_servers[base.to_s]
    base
  end

  attr_accessor :pkgdir, :subdir, :resources, :rake_dirs, :remote_server

  def root()
    Path.setup @subdir || "", @pkgdir, self
  end

  def method_missing(name, prev = nil, *args)
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

  def get_from_server(path, final_path)
    url = File.join(remote_server, '/resource/', self.to_s, 'get_file')
    url << "?" << Misc.hash2GET_params(:file => path, :create => false)
    begin
      Open.write(final_path, Open.read(url, :nocache => true))
      return true
    rescue
      Log.warn "Could not retrieve (#{self.to_s}) #{ path } from #{ remote_server }"
      Log.error $!.message
      FileUtils.rm final_path if File.exists? final_path
      return false
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
      Log.medium "Producing: #{ final_path }"
      Misc.lock final_path + '.produce' do
        (remote_server and get_from_server(path, final_path)) or
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
