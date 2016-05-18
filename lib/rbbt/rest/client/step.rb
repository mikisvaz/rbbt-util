class WorkflowRESTClient
  class RemoteStep < Step

    attr_accessor :url, :base_url, :task, :base_name, :inputs, :result_type, :result_description, :is_exec, :stream_input

    def self.get_streams(inputs)
      new_inputs = {}
      inputs.each do |k,v|
        if Step === v 
          new_inputs[k] = TSV.get_stream v
        else
          new_inputs[k] = v
        end
      end
      new_inputs
    end

    def get_streams
      @inputs = WorkflowRESTClient::RemoteStep.get_streams @inputs
    end


    def initialize(base_url, task = nil, base_name = nil, inputs = nil, result_type = nil, result_description = nil, is_exec = false, stream_input = nil)
      @base_url, @task, @base_name, @inputs, @result_type, @result_description, @is_exec = base_url, task, base_name, inputs, result_type, result_description, is_exec
      @mutex = Mutex.new
      @stream_input = stream_input
      #@inputs = RemoteStep.get_streams @inputs
    end

    def dup_inputs
      return if @dupped or ENV["RBBT_NO_STREAM"] == 'true'
      Log.low "Dupping inputs for remote #{path}"
      new_inputs = {}
      @inputs.each do |name,input|
        new_inputs[name] = Step.dup_stream input
      end
      @inputs = RemoteStep.get_streams new_inputs
      @dupped = true
    end

    def name
      return nil if @is_exec
      return @path if @url.nil?
      (Array === @url ? @url.first : @url).split("/").last
    end

    def task_name
      return task if task
      init_job
      (Array === @url ? @url.first : @url).split("/")[-2]
    end

    def info(check_lock=false)
      @done = @info and @info[:status] and @info[:status].to_sym == :done
      @info = Persist.memory("RemoteSteps Info", :url => @url, :persist => !!@done) do
        init_job unless @url
        info = WorkflowRESTClient.get_json(File.join(@url, 'info'))
        info = WorkflowRESTClient.fix_hash(info)
        info[:status] = info[:status].to_sym if String === info[:status]
        info
      end
    end

    def status
      return nil unless url or started?
      begin
        info[:status]
      ensure
        @info = nil
      end
    end

    def started?
      @result != nil or @started
    end

    def done?
      @done || status.to_s == 'done'
    end

    def files
      WorkflowRESTClient.get_json(File.join(url, 'files'))
    end

    def file(file)
      WorkflowRESTClient.get_raw(File.join(url, 'file', file))
    end

    def get_stream
      case @result
      when IO 
        @result
      when String
        StringIO.new @result
      else
        nil
      end
    end

    def grace
    end

    #{{{ MANAGEMENT

    def init_job(cache_type = nil, other_params = {})
      Log.stack caller
      cache_type = :asynchronous if cache_type.nil? and not @is_exec
      cache_type = :exec if cache_type.nil?
      @name ||= Persist.memory("RemoteSteps", :workflow => self, :task => task, :jobname => @name, :inputs => inputs, :cache_type => cache_type) do
        WorkflowRESTClient.post_jobname(File.join(base_url, task.to_s), inputs.merge(other_params).merge(:jobname => @name||@base_name, :_cache_type => cache_type))
      end
      @url = File.join(base_url, task.to_s, @name)
      nil
    end


    def fork
      init_job(:asynchronous)
    end

    def running?
      ! %w(done error aborted).include? status.to_s
    end

    def path
      if @url
        @url + '?_format=raw'
      else
        [base_url, task, Misc.fingerprint(inputs)] * "/"
      end
    end

    def run(noload = false)
      @mutex.synchronize do
        @result ||= begin
                      if @is_exec
                        exec(noload)
                      elsif noload == :stream
                        _run_job(:stream)
                      else
                        init_job 
                        self.load
                      end
                    ensure
                      @started = true
                    end
      end

      return @result if noload == :stream
      noload ? path + '?_format=raw' : @result
    end

    def exec(noload = false)
      @result ||= begin
                     if noload == :stream
                       _run_job(:exec)
                     else
                       exec_job 
                     end
                    ensure
                      @started = true
                   end
    end

    def join
      sleep 0.2 unless self.done?
      sleep 1 unless self.done?
      sleep 3 while not self.done?
      self
    end

    def get
      params ||= {}
      params = params.merge(:_format => [:string, :boolean, :tsv, :annotations,:array].include?(result_type.to_sym) ? :raw : :json )
      Misc.insist 3, rand(2) + 1 do
        begin
          WorkflowRESTClient.get_raw(url, params)
        rescue
          Log.exception $!
          raise $!
        end
      end
    end

    def load_res(res, result_type = nil)
      stream = true if res.respond_to? :read
      join unless stream
      result_type ||= self.result_type
      case result_type
      when :string
        stream ? res.read : res
      when :boolean
        (stream ? res.read : res) == 'true'
      when :tsv
        if stream
          TSV.open(res, :monitor => true)
        else
          TSV.open(StringIO.new(res))
        end
      when :annotations
        if stream
          Annotated.load_tsv(TSV.open(res))
        else
          Annotated.load_tsv(TSV.open(StringIO.new(res)))
        end
      when :array
        (stream ? res.read : res).split("\n")
        res.split("\n")
      else
        JSON.parse res
      end
    end

    def load
      params = {}
      load_res get
    end
    
    def _stream_job(stream_input, cache_type = :exec)
      require 'rbbt/util/misc/multipart_payload'
      WorkflowRESTClient.capture_exception do
        task_url = URI.encode(File.join(base_url, task.to_s))
        Log.debug{ "RestClient stream: #{ task_url } #{stream_input} #{cache_type} - #{Misc.fingerprint inputs}" }
        task_params = inputs.merge(:_cache_type => cache_type, :jobname => base_name, :_format => [:string, :boolean, :tsv, :annotations].include?(result_type) ? :raw : :json)
        res = RbbtMutiplartPayload.issue task_url, task_params, stream_input, nil, nil, true
        type = res.gets
        case type.strip
        when "LOCATION"
          @url = res.gets
          @url.sub!(/\?.*/,'')
          WorkflowRESTClient.get_raw(@url)
        when /STREAM: (.*)/
          @url = $1.strip
          ConcurrentStream.setup(res)
          res.callback = Proc.new do
            @done = true
          end
          res
        when "BULK"
          begin
            res.read
          ensure
            @done = true
          end
        else
          raise "What? " + type
        end
      end
    end

    def _run_job(cache_type = :async)
      #if cache_type == :stream and stream_input
      if cache_type == :stream or cache_type == :exec and stream_input
        return _stream_job(stream_input, cache_type) 
      end
      WorkflowRESTClient.capture_exception do
        @url = URI.encode(File.join(base_url, task.to_s))
        task_params = inputs.merge(:_cache_type => cache_type, :jobname => base_name, :_format => [:string, :boolean, :tsv, :annotations].include?(result_type) ? :raw : :json)

        sout, sin = Misc.pipe
        streamer = lambda do |c|
          sin.write c
        end

        Thread.new do
          bl = lambda do |rok|
            rok.read_body do |c,_a, _b|
              sin.write c
            end
            sin.close
          end

          RestClient::Request.execute(:method => :post, :url => url, :payload => task_params, :block_response => bl)
        end

        reader = Zlib::GzipReader.new(sout)
        Misc.open_pipe do |sin|
          while c = reader.read(1015)
            sin.write c
          end
          sin.close
          @done = true
        end
      end
    end

    def exec_job
      res = _run_job(:exec)
      load_res res, result_type == :array ? :json : result_type
    end

    def _restart
      @done = nil
      @name = nil
      @started = nil
      new_inputs = {}
      inputs.each do |k,i| 
        if File === i 
          new_inputs[k] = File.open(i.path)
        else
          new_inputs[k] = i
        end
      end
      @inputs = new_inputs
    end

    def recursive_clean
      begin
        params = {:_update => :recursive_clean}
        init_job(nil, params)
        WorkflowRESTClient.get_raw(url, params)
        _restart
      rescue Exception
        Log.exception $!
      end
      self
    end

    def clean
      begin
        params = {:_update => :clean}
        init_job(nil, params)
        WorkflowRESTClient.get_raw(url, params)
        _restart
      rescue Exception
        Log.exception $!
      end
      self
    end
  end
end
