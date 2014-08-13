require 'rbbt/util/open'
require 'rbbt/util/log'
require 'rbbt/resource/path'
require 'net/http'
require 'set'

 
module Resource

  class << self
    attr_accessor :lock_dir
    
    def lock_dir
      @lock_dir ||= Rbbt.tmp.produce_locks.find
    end
  end

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

  attr_accessor :server_missing_resource_cache
  def get_from_server(path, final_path, remote_server = nil)
    remote_server ||= self.remote_server
    url = File.join(remote_server, '/resource/', self.to_s, 'get_file')
    url << "?" << Misc.hash2GET_params(:file => path, :create => false)

    begin
      @server_missing_resource_cache ||= Set.new
      raise "Resource Not Found" if @server_missing_resource_cache.include? url
      lock_filename = Persist.persistence_path(final_path, {:dir => Resource.lock_dir})
      Misc.lock lock_filename do
        Net::HTTP.get_response URI(url) do |response|
          case response
          when Net::HTTPSuccess, Net::HTTPOK
            Misc.sensiblewrite(final_path) do |file|
              response.read_body do |chunk|
                file.write chunk
              end
            end
          when Net::HTTPRedirection, Net::HTTPFound
            location = response['location']
            Log.debug("Feching directory from: #{location}. Into: #{final_path}")
            FileUtils.mkdir_p final_path unless File.exists? final_path
            TmpFile.with_file do |tmp_dir|
              Misc.in_dir tmp_dir do
                CMD.cmd('tar xvfz -', :in => Open.open(location, :nocache => true))
              end
            end
            File.utils tmp_dir, final_path
          when Net::HTTPInternalServerError
            @server_missing_resource_cache << url
            raise "Resource Not Found"
          else
            raise "Response not understood: #{response.inspect}"
          end
        end
      end
    rescue
      Log.warn "Could not retrieve (#{self.to_s}) #{ path } from #{ remote_server }"
      Log.error $!.message
      FileUtils.rm_rf final_path if File.exists? final_path
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
      raise "Resource is missing and does not seem to be claimed: #{ self } -- #{ path } "
    end

    final_path = path.respond_to?(:find) ? (force ? path.find(:user) : path.find) : path
    if not File.exists? final_path or force
      Log.medium "Producing: #{ final_path }"
      lock_filename = Persist.persistence_path(final_path, {:dir => Resource.lock_dir})
      Misc.lock lock_filename do
        if not File.exists? final_path or force
          (remote_server and get_from_server(path, final_path)) or
          begin
            case type
            when :string
              Misc.sensiblewrite(final_path, content)
            when :url
              Misc.sensiblewrite(final_path, Open.open(content))
            when :proc
              data = case content.arity
                     when 0
                       content.call
                     when 1
                       content.call final_path
                     end
              Misc.sensiblewrite(final_path, data) unless data.nil?
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
    end

    path
  end
end

