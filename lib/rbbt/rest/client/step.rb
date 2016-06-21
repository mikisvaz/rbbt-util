class WorkflowRESTClient

  def self.__prepare_inputs_for_restclient(inputs)

    new = IndiferentHash.setup({})
    inputs.each do |k,v|
      if v.respond_to? :path and not v.respond_to? :original_filename
        class << v
          def original_filename
            File.expand_path(path)
          end
        end
      end

      if Array === v and v.empty?
        new[k] = "EMPTY_ARRAY"
      else
        new[k] = v
      end
    end

    new
  end

  class RemoteStep < Step

    attr_accessor :url, :base_url, :task, :base_name, :inputs, :result_type, :result_description, :is_exec, :stream_input

    def initialize(base_url, task = nil, base_name = nil, inputs = nil, result_type = nil, result_description = nil, is_exec = false, stream_input = nil)
      @base_url, @task, @base_name, @inputs, @result_type, @result_description, @is_exec, @stream_input = base_url, task, base_name, inputs, result_type, result_description, is_exec, stream_input
      @mutex = Mutex.new
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


    def self.get_streams(inputs, stream_input = nil)
      new_inputs = {}
      inputs.each do |k,v|
        stream = stream_input.to_s == k.to_s
        if Step === v 
          unless (v.done? or v.streaming? or RestClient::Step === v)
            v.run(true) and v.grace 
          end

          begin
            if stream
              new_inputs[k] = TSV.get_stream(v)
            else
              new_inputs[k] = v.load
            end
          rescue Exception
            raise $!
          end
        else
          new_inputs[k] = v
        end
      end
      new_inputs
    end

    def get_streams
      return if @inputs_done
      @inputs = WorkflowRESTClient::RemoteStep.get_streams @inputs, @stream_input
      @inputs_done = true
      @inputs
    end

    def abort
      #WorkflowRESTClient.get_json(File.join(@url, '?_update=abort')) if @url
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
      return :done if @done
      return :streaming if @streaming
      begin
        status = info[:status]
        @done = true if status and status.to_sym == :done
        status
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
      cache_type = :asynchronous if cache_type.nil? and not @is_exec
      cache_type = :exec if cache_type.nil?
      get_streams
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
      if IO === @result
        res = @result
        @result = nil
        Misc.consume_stream(res, true) 
      end
      sleep 0.2 unless self.done?
      sleep 1 unless self.done?
      while not self.done?
        sleep 3
      end
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

    def _run_job(cache_type = :async)
      get_streams
      if cache_type == :stream or cache_type == :exec and stream_input and inputs[stream_input]
        task_url = URI.encode(File.join(base_url, task.to_s))
        inputs = WorkflowRESTClient.__prepare_inputs_for_restclient(inputs)
        task_params = inputs.merge(:_cache_type => cache_type, :jobname => base_name, :_format => [:string, :boolean, :tsv, :annotations].include?(result_type) ? :raw : :json)
        @streaming = true
        io =  WorkflowRESTClient.stream_job(task_url, task_params, stream_input, cache_type) 
        if IO === io
          ConcurrentStream.setup(io)
          io.add_callback do
            @done = true
            @streaming = false
          end
        else
          @done = true
          @streaming = false
        end

        @url = io.filename if io.filename
        return io
      end

      WorkflowRESTClient.capture_exception do
        @url = URI.encode(File.join(base_url, task.to_s))
        task_params = inputs.merge(:_cache_type => cache_type, :jobname => base_name, :_format => [:string, :boolean, :tsv, :annotations].include?(result_type) ? :raw : :json)

        sout, sin = Misc.pipe
        streamer = lambda do |c|
          sin.write c
        end

        post_thread = Thread.new(Thread.current) do |parent|
          bl = lambda do |rok|
            if Net::HTTPOK === rok
              _url = rok["RBBT-STREAMING-JOB-URL"]
              @url = File.join(@url, File.basename(_url)) if _url
              rok.read_body do |c,_a, _b|
                sin.write c
              end
              sin.close
            else
              parent.raise "Error in RestClient: " << rok.message
            end
          end

          Log.debug{ "RestClient execute: #{ url } - #{Misc.fingerprint task_params}" }
          RestClient::Request.execute(:method => :post, :url => url, :payload => task_params, :block_response => bl)
        end

        reader = Zlib::GzipReader.new(sout)
        res_io = Misc.open_pipe do |sin|
          while c = reader.read(Misc::BLOCK_SIZE)
            sin.write c
          end
          sin.close
          @done = true
        end

        ConcurrentStream.setup(res_io, :threads => [post_thread])
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
      return
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
      return
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
