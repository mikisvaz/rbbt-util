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
    base.search_paths = Path::SEARCH_PATHS.dup
    base.remote_server = Resource.remote_servers[base.to_s]
    base
  end

  attr_accessor :pkgdir, :subdir, :resources, :rake_dirs, :remote_server, :search_paths

  def set_libdir(value = nil)
    _libdir = value || Path.caller_lib_dir
    search_paths.merge!(:lib => File.join(_libdir, '{TOPLEVEL}', '{SUBPATH}'))
  end

  def root
    Path.setup @subdir || "", @pkgdir, self, @search_paths
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
    remote_server = "http://" + remote_server unless remote_server =~ /^[a-z]+:\/\//
    url = File.join(remote_server, '/resource/', self.to_s, 'get_file')
    url << "?" << Misc.hash2GET_params(:file => path, :create => false)

    begin
      @server_missing_resource_cache ||= Set.new
      raise "Resource Not Found" if @server_missing_resource_cache.include? url
      
      #lock_filename = Persist.persistence_path(final_path, {:dir => Resource.lock_dir})
      
      lock_filename = nil # it seems like this was locked already.

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
            FileUtils.mkdir_p final_path unless File.exist? final_path
            TmpFile.with_file do |tmp_dir|
              Misc.in_dir tmp_dir do
                CMD.cmd('tar xvfz -', :in => Open.open(location, :nocache => true))
              end
              FileUtils.mv tmp_dir, final_path
            end
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
      FileUtils.rm_rf final_path if File.exist? final_path
      return false
    end
  end

  def produce(path, force = false)
    case
    when @resources.include?(path)
      type, content = @resources[path]
    when @resources.include?(path.original)
      type, content = @resources[path.original]
    when has_rake(path)
      type = :rake
      rake_dir, content = rake_for(path)
      rake_dir = Path.setup(rake_dir.dup, self.pkgdir, self)
    else
      raise "Resource is missing and does not seem to be claimed: #{ self } -- #{ path } "
    end

    if path.respond_to?(:find) 
      final_path = force ? path.find(:default) : path.find
    else
      final_path = path
    end

    if not File.exist? final_path or force
      Log.medium "Producing: #{ final_path }"
      lock_filename = Persist.persistence_path(final_path, {:dir => Resource.lock_dir})
      Misc.lock lock_filename do
        FileUtils.rm_rf final_path if force and File.exist? final_path
        if not File.exist? final_path or force
          (remote_server and get_from_server(path, final_path)) or
          begin
            case type
            when :string
              Misc.sensiblewrite(final_path, content)
            when :url
              options = {}
              options[:noz] = true if Open.gzip?(final_path) || Open.bgzip?(final_path) || Open.zip?(final_path)
              Misc.sensiblewrite(final_path, Open.open(content, options))
            when :proc
              data = case content.arity
                     when 0
                       content.call
                     when 1
                       content.call final_path
                     end
              case data
              when String, IO, StringIO
                Misc.sensiblewrite(final_path, data) 
              when Array
                Misc.sensiblewrite(final_path, data * "\n")
              when TSV
                Misc.sensiblewrite(final_path, data.dumper_stream) 
              when TSV::Dumper
                Misc.sensiblewrite(final_path, data.stream) 
              when nil
              else
                raise "Unkown object produced: #{Misc.fingerprint data}"
              end
            when :rake
              run_rake(path, content, rake_dir)
            when :install
              Log.debug "Installing software: #{path}"
              software_dir = path.resource.root.software.find :user
              helper_file = File.expand_path(Rbbt.share.install.software.lib.install_helpers.find(:lib, caller_lib_dir(__FILE__)))
              #helper_file = File.expand_path(Rbbt.share.install.software.lib.install_helpers.find)

              preamble = <<-EOF
#!/bin/bash

RBBT_SOFTWARE_DIR="#{software_dir}"

INSTALL_HELPER_FILE="#{helper_file}"
source "$INSTALL_HELPER_FILE"
              EOF

              script = preamble + "\n" + Open.read(content)
              CMD.cmd_log('bash', :in => script)

              set_software_env(software_dir)
            else
              raise "Could not produce #{ resource }. (#{ type }, #{ content })"
            end
          rescue
            FileUtils.rm_rf final_path if File.exist? final_path
            raise $!
          end
        end
      end
    end

    path
  end
end

