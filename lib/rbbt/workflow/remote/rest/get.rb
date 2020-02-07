module WorkflowRESTClient
  def self.encode(url)
    begin
      URI.encode(url)
    rescue
      Log.warn $!.message
      url
    end
  end

  def self.fix_hash(hash, fix_values = false)
    fixed = {}
    hash.each do |key, value|
      fixed[key.to_sym] = case value
                          when TrueClass
                            value
                          when FalseClass
                            value
                          when Hash 
                            fix_hash(value)  
                          when (fix_values and String )
                            value.to_sym
                          when IO
                            value.read
                          when TSV::Dumper
                            value.stream
                          when Step
                            stream = get_stream(value)
                            stream || value.load
                          else
                            value
                          end
    end
    fixed
  end

  def self.parse_exception(text)
    klass, message = text.split " => "
    begin
      klass = Kernel.const_get klass
      return klass.new message
    rescue
      message
    end
  end

  def self.capture_exception
    begin
      yield
    rescue Exception => e
      raise e unless e.respond_to? :response
      begin
        ne = parse_exception e.response.to_s
        case ne
        when String
          raise e.class, ne
        when Exception
          raise ne
        else
          raise
        end
      rescue
        raise e
      end
      raise $!
    end
  end

  def self.fix_params(params)
    new_params = {}
    params.each do |k,v|
      if Array === v and v.empty?
        new_params[k] = "EMPTY_ARRAY"
      else
        new_params[k] = v
      end
    end
    new_params
  end

  def self.clean_url(url, params = {})
    params = params.merge({ :_format => 'json', :update => 'clean' })
    params = fix_params params
    res = capture_exception do
      Misc.insist(2, 0.5) do
        Log.debug{ "RestClient clean: #{ url } - #{Misc.fingerprint params}" }
        res = begin 
                RestClient.get(self.encode(url), :params => params)
              rescue RestClient::NotFound
                return nil
              end
        raise TryAgain if res.code == 202
        res
      end
    end
    res
  end
 
  def self.get_raw(url, params = {})
    params = params.merge({ :_format => 'raw' })
    params = fix_params params
    res = capture_exception do
      Misc.insist(2, 0.5) do
        raise "No url" if url.nil?
        Log.debug{ "RestClient get_raw: #{ url } - #{Misc.fingerprint params}" }
        res = RestClient.get(self.encode(url), :params => params)
        raise TryAgain if res.code == 202
        res.to_s
      end
    end
    res
  end
 
  def self.get_json(url, params = {})
    Log.debug{ "RestClient get_json: #{ url } - #{Misc.fingerprint params }" }
    params = params.merge({ :_format => 'json' })
    params = fix_params params

    res = capture_exception do
      Misc.insist(2, 0.5) do
        RestClient.get(self.encode(url), :params => params)
      end
    end

    begin
      JSON.parse(res)
    rescue
      res
    end
  end

  def self.post_jobname(url, params = {})
    Log.debug{ "RestClient post_jobname: #{ url } - #{Misc.fingerprint params}" }
    params = params.merge({ :_format => 'jobname' })
    params = fix_params params

    WorkflowRESTClient.__prepare_inputs_for_restclient(params)
    name = capture_exception do
      RestClient.post(self.encode(url), params)
    end

    Log.debug{ "RestClient jobname returned for #{ url } - #{Misc.fingerprint params}: #{name}" }

    name
  end

  def self.post_json(url, params = {})
    if url =~ /_cache_type=:exec/
      JSON.parse(Open.open(url, :nocache => true))
    else
      params = params.merge({ :_format => 'json' })
      params = fix_params params

      res = capture_exception do
        RestClient.post(self.encode(url), params)
      end

      begin
        JSON.parse(res)
      rescue
        res
      end
    end
  end

  def get
    params ||= {}
    params = params.merge(:_format => [:string, :boolean, :tsv, :annotations, :array].include?(result_type.to_sym) ? :raw : :json )
    @cache_result ||= Persist.persist("REST persist", :binary, :file => cache_file + "." + Misc.obj2digest(params)) do
      Misc.insist 3, rand(2) + 1 do
        begin
          init_job if url.nil?
          @adaptor.get_raw(url, params)
        rescue
          Log.exception $!
          raise $!
        end
      end
    end
  end

  def load
    params = {}
    join unless done? or streaming?
    raise get_exception if error? or aborted?
    load_res get, (result_type == :array ? :json : result_type)
  end

  def exec_job
    res = _run_job(:exec)
    load_res res, result_type == :array ? :json : result_type
  end

  def abort
    return self if status == :done
    @adaptor.get_json(@url + '?_update=abort') if @url and @name
    self
  end

  def init_job(cache_type = nil, other_params = {})
    cache_type = :asynchronous if cache_type.nil? and not @is_exec
    cache_type = :exec if cache_type.nil?
    @last_info_time = nil
    @done = false
    get_streams
    @name ||= Persist.memory("RemoteSteps", :workflow => self, :task => task, :jobname => @name, :inputs => inputs, :cache_type => cache_type) do
      Misc.insist do
        @adaptor.post_jobname(File.join(base_url, task.to_s), inputs.merge(other_params).merge(:jobname => @name||@base_name, :_cache_type => cache_type))
      end
    end
    if Open.remote? @name
      @url = @name
      @name = File.basename(@name)
    else
      @url = File.join(base_url, task.to_s, @name)
    end
    self
  end

  def recursive_clean
    Log.warn "Not doing recursive cleans"
    return
    begin
      _restart
      params = {:_update => :recursive_clean}
      @adaptor.get_raw(url, params)
    rescue Exception
      Log.exception $!
    end
    self
  end

  def _clean
    begin
      _restart
      cache_files.each do |cache_file|
        Open.rm cache_file
      end
      params = {:_update => :clean}
      @adaptor.clean_url(url, params) if @url
    rescue Exception
      Log.exception $!
    end
  end

  def clean
    init_job
    _clean
    self
  end

  def stream_job(task_url, task_params, stream_input, cache_type = :exec)
    require 'rbbt/util/misc/multipart_payload'
    WorkflowRESTClient.capture_exception do
      @streaming = true

      Log.debug{ "RestClient stream #{Process.pid}: #{ task_url } #{stream_input} #{cache_type} - #{Misc.fingerprint task_params}" }
      res = RbbtMutiplartPayload.issue task_url, task_params, stream_input, nil, nil, true
      type = res.gets

      out = case type.strip
            when "LOCATION"
              @url = res.gets
              @url.sub!(/\?.*/,'')
              join
              WorkflowRESTClient.get_raw(@url)
              @done = true
              @streaming = false
            when /STREAM: (.*)/
              @url = $1.strip
              res.callback = Proc.new do
                Log.medium "Done streaming result from #{@url}"
                @done = true
                @streaming = false
              end
              res
            when "BULK"
              begin
                res.read
              ensure
                @done = true
                @streaming = false
              end
            else
              raise "What? " + type
            end

      ConcurrentStream.setup(out, :filename => @url)

      out
    end
  end

  def _run_job(cache_type = :async)
    get_streams

    task_url = URI.encode(File.join(base_url, task.to_s))
    @adaptor.__prepare_inputs_for_restclient(inputs)
    task_params = inputs.merge(:_cache_type => cache_type, :jobname => base_name, :_format => [:string, :boolean, :tsv, :annotations].include?(result_type) ? :raw : :json)

    if cache_type == :stream or cache_type == :exec and stream_input and inputs[stream_input]
      io =  self.stream_job(task_url, task_params, stream_input, cache_type) 
      return io
    else
      @adaptor.execute_job(base_url, task, task_params, cache_type)
    end

  end

end
