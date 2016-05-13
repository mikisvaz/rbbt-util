class WorkflowRESTClient
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

  def self.capture_exception
    begin
      yield
    rescue Exception => e
      raise e unless e.respond_to? :response
      begin
        klass, message = e.response.to_s.split " => "
        klass = Kernel.const_get klass
        raise klass.new message
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

  def self.get_raw(url, params = {})
    Log.debug{ "RestClient get_raw: #{ url } - #{Misc.fingerprint params}" }
    params = params.merge({ :_format => 'raw' })
    params = fix_params params
    res = capture_exception do
      Misc.insist(2, 0.5) do
        res = RestClient.get(URI.encode(url), :params => params)
        raise TryAgain if res.code == 202
        res
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
        RestClient.get(URI.encode(url), :params => params)
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

    name = capture_exception do
      RestClient.post(URI.encode(url), params)
    end

    Log.debug{ "RestClient post_jobname: #{ url } - #{Misc.fingerprint params}: #{name}" }

    name
  end

  def self.post_json(url, params = {})
    if url =~ /_cache_type=:exec/
      JSON.parse(Open.open(url, :nocache => true))
    else
      params = params.merge({ :_format => 'json' })
      params = fix_params params

      res = capture_exception do
        RestClient.post(URI.encode(url), params)
      end

      begin
        JSON.parse(res)
      rescue
        res
      end
    end
  end

end
