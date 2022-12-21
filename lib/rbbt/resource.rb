require 'rbbt/util/open'
require 'rbbt/util/log'
require 'rbbt/resource/path'
require 'net/http'
require 'set'

module Resource
  class ResourceNotFound < RbbtException; end

 
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
    base.remote_server = Resource.remote_servers[base.to_s] || Resource.remote_servers["*"]
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
      @rake_dirs[path] = content || block
    else
      @resources[path] = [type, content || block]

      if type == :install
        software_dir = path.resource.root.software
        set_software_env(software_dir) unless $set_software_env
        $set_software_env = true
      end
    end
  end

  def claims
    resources.keys.collect{|path| path.sub(subdir, '').sub(/^\//,'') }
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

      Log.low "Downloading #{path} from #{url} file server"
      Misc.lock lock_filename do
        begin
          uri = URI(url)

          http = Net::HTTP.new(uri.host, uri.port)

          if uri.scheme == "https"
            http.use_ssl = true
            http.verify_mode = OpenSSL::SSL::VERIFY_NONE
            http.instance_variable_set("@ssl_options", OpenSSL::SSL::OP_NO_SSLv2 + OpenSSL::SSL::OP_NO_SSLv3 + OpenSSL::SSL::OP_NO_COMPRESSION)
          end

          timeout = 60 * 10
          http.read_timeout = timeout
          http.open_timeout = timeout
          request = Net::HTTP::Get.new(uri.request_uri)
          http.request request do |response|
            filename = if response["Content-Disposition"] 
                         response["Content-Disposition"].split(";").select{|f| f.include? "filename"}.collect{|f| f.split("=").last.gsub('"','')}.first
                       else
                         nil
                       end

            if filename && filename =~ /\.b?gz$/ && final_path !~ /\.b?gz$/
              extension = filename.split(".").last
              final_path += '.' + extension
            end
            case response
            when Net::HTTPSuccess, Net::HTTPOK
              Misc.sensiblewrite(final_path) do |file|
                response.read_body do |chunk|
                  file.write chunk
                end
              end
            when Net::HTTPRedirection, Net::HTTPFound
              location = response['location']
              if location.include? 'get_directory'
                Log.debug("Feching directory from: #{location}. Into: #{final_path}")
                FileUtils.mkdir_p final_path unless File.exist? final_path
                TmpFile.with_file do |tmp_dir|
                  Misc.in_dir tmp_dir do
                    CMD.cmd('tar xvfz -', :in => Open.open(location, :nocache => true))
                  end
                  FileUtils.mv tmp_dir, final_path
                end
              else
                url = location
                raise TryAgain
                #Open.open(location, :nocache => true) do |s|
                #  Misc.sensiblewrite(final_path, s)
                #end
              end
            when Net::HTTPInternalServerError
              @server_missing_resource_cache << url
              raise "Resource Not Found"
            else
              raise "Response not understood: #{response.inspect}"
            end
          end
        rescue TryAgain
          retry
        end
      end
    rescue
      Log.warn "Could not retrieve (#{self.to_s}) #{ path } from #{ remote_server }"
      Log.error $!.message
      FileUtils.rm_rf final_path if File.exist? final_path
      return false
    end

    final_path
  end

  def produce(path, force = false)
    case
    when @resources.include?(path)
      type, content = @resources[path]
    when (Path === path && @resources.include?(path.original))
      type, content = @resources[path.original]
    when has_rake(path)
      type = :rake
      rake_dir, content = rake_for(path)
      rake_dir = Path.setup(rake_dir.dup, self.pkgdir, self)
    else
      if path !~ /\.(gz|bgz)$/
        begin
          produce(path.annotate(path + '.gz'), force)
        rescue ResourceNotFound
          begin
            produce(path.annotate(path + '.bgz'), force)
          rescue ResourceNotFound
            raise ResourceNotFound, "Resource is missing and does not seem to be claimed: #{ self } -- #{ path } "
          end
        end
      else
        raise ResourceNotFound, "Resource is missing and does not seem to be claimed: #{ self } -- #{ path } "
      end
    end

    if path.respond_to?(:find) 
      final_path = force ? path.find(:default) : path.find
    else
      final_path = path
    end

    if type and not File.exist?(final_path) or force
      Log.medium "Producing: (#{self.to_s}) #{ final_path }"
      lock_filename = Persist.persistence_path(final_path, {:dir => Resource.lock_dir})

      Misc.lock lock_filename do
        FileUtils.rm_rf final_path if force and File.exist? final_path

        if ! File.exist?(final_path) || force 

          begin
            case type
            when :string
              Misc.sensiblewrite(final_path, content)
            when :csv
              require 'rbbt/tsv/csv'
              tsv = TSV.csv Open.open(content)
              Misc.sensiblewrite(final_path, tsv.to_s)
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

              $set_software_env = false unless File.exist? path
              
              software_dir = path.resource.root.software.find :user
              helper_file = File.expand_path(Rbbt.share.install.software.lib.install_helpers.find(:lib, caller_lib_dir(__FILE__)))
              #helper_file = File.expand_path(Rbbt.share.install.software.lib.install_helpers.find)

              preamble = <<-EOF
#!/bin/bash

RBBT_SOFTWARE_DIR="#{software_dir}"

INSTALL_HELPER_FILE="#{helper_file}"
source "$INSTALL_HELPER_FILE"
              EOF

              content = content.call if Proc === content

              content = if content =~ /git:|\.git$/
                          {:git => content}
                        else
                          {:src => content}
                        end if String === content and Open.remote?(content)

              script_text = case content
                            when nil
                              raise "No way to install #{path}"
                            when Path
                              Open.read(content) 
                            when String
                              if Misc.is_filename?(content) and Open.exists?(content)
                                Open.read(content) 
                              else
                                content
                              end
                            when Hash
                              name = content[:name] || File.basename(path)
                              git = content[:git]
                              src = content[:src]
                              url = content[:url]
                              jar = content[:jar]
                              extra = content[:extra]
                              commands = content[:commands]
                              if git
                                <<-EOF

name='#{name}'
url='#{git}'

install_git "$name" "$url" #{extra}

#{commands}
                                EOF
                              elsif src
                                <<-EOF

name='#{name}'
url='#{src}'

install_src "$name" "$url" #{extra}

#{commands}
                                EOF
                              elsif jar
                                <<-EOF

name='#{name}'
url='#{jar}'

install_jar "$name" "$url" #{extra}

#{commands}
                                EOF
                              else
                                <<-EOF

name='#{name}'
url='#{url}'

#{commands}
                                EOF
                              end
                            end

              script = preamble + "\n" + script_text
              Log.debug "Installing software with script:\n" << script
              CMD.cmd_log('bash', :in => script)

              set_software_env(software_dir) unless $set_software_env
              $set_software_env = true
            else
              raise "Could not produce #{ resource }. (#{ type }, #{ content })"
            end
          rescue
            FileUtils.rm_rf final_path if File.exist? final_path
            raise $!
          end unless (remote_server && get_from_server(path, final_path))
        end
      end
    end

    # After producing a file, make sure we recheck all locations, the file
    # might have appeared with '.gz' extension for instance
    path.instance_variable_set("@path", {})

    path
  end

  def identify(path)
    path = File.expand_path(path)
    resource ||= Rbbt
    locations = (Path::STANDARD_SEARCH + resource.search_order + resource.search_paths.keys)
    locations -= [:current, "current"]
    locations << :current
    search_paths = IndiferentHash.setup(resource.search_paths)
    locations.uniq.each do |name|
      pattern = search_paths[name]
      pattern = resource.search_paths[pattern] while Symbol === pattern
      next if pattern.nil?

      pattern = pattern.sub('{PWD}', Dir.pwd)
      if String ===  pattern and pattern.include?('{')
        regexp = "^" + pattern.gsub(/{([^}]+)}/,'(?<\1>[^/]+)') + "(?:/(?<REST>.*))?/?$"
        if m = path.match(regexp) 
          if ! m.named_captures.include?("PKGDIR") || m["PKGDIR"] == resource.pkgdir
            return self[m["TOPLEVEL"]][m["SUBPATH"]][m["REST"]]
          end
        end
      end
    end
    nil
  end
end

