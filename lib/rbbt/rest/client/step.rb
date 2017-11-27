class WorkflowRESTClient

  def self.__prepare_inputs_for_restclient(inputs)
    inputs.each do |k,v|
      if v.respond_to? :path and not v.respond_to? :original_filename
        class << v
          def original_filename
            File.expand_path(path)
          end
        end
      end

      if Array === v and v.empty?
        inputs[k] = "EMPTY_ARRAY"
      end
    end
  end

  class RemoteStep < Step

    attr_accessor :url, :base_url, :task, :base_name, :inputs, :result_type, :result_description, :is_exec, :is_stream, :stream_input

    def initialize(base_url, task = nil, base_name = nil, inputs = nil, result_type = nil, result_description = nil, is_exec = false, is_stream = false, stream_input = nil)
      @base_url, @task, @base_name, @inputs, @result_type, @result_description, @is_exec, @is_stream, @stream_input = base_url, task, base_name, inputs, result_type, result_description, is_exec, is_stream, stream_input
      @base_url = "http://" << @base_url unless @base_url =~ /^https?:\/\//
      @mutex = Mutex.new
    end

    def clean_name
      @base_name
    end

    def run(no_load = false)
      no_load = @is_stream ? :stream : true if no_load

      @mutex.synchronize do
        @result ||= begin
                      if @is_exec
                        exec(no_load)
                      elsif no_load == :stream
                        _run_job(:stream)
                      elsif no_load
                        init_job 
                        nil
                      else
                        init_job 
                        join
                        self.load
                      end
                    ensure
                      @started = true
                    end
      end

      return @result if no_load == :stream
      no_load ? Misc.add_GET_param(path, "_format", "raw") : @result
    end


    def self.get_streams(inputs, stream_input = nil)
      new_inputs = {}
      inputs.each do |k,v|
        stream = stream_input.to_s == k.to_s
        if Step === v 
          unless (v.done? or v.streaming?) # or RestClient::Step === v)
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
      return self if status == :done
      WorkflowRESTClient.get_json(@url + '?_update=abort') if @url and @name
      self
    end

    def dup_inputs
      return if @dupped or ENV["RBBT_NO_STREAM"] == 'true'
      Log.low "Dupping inputs for #{path}"
      dupped_inputs = {}
      @inputs.collect do |k,input|
        dupped_inputs[k] = Step.dup_stream input
      end
      @inputs = dupped_inputs
      @dupped = true
    end

    def name
      return nil if @is_exec
      return @path if @url.nil?
      (Array === @url ? @url.first : @url).split("/").last
    end

    def name=(name)
      @url = [base_url,task, name] * "/"
    end

    def task_name
      return task if task
      init_job
      (Array === @url ? @url.first : @url).split("/")[-2]
    end
    
    def nopid?
      false
    end

    def info(check_lock=false)
      @done = @info && @info[:status] && @info[:status].to_sym == :done
      @info = Persist.memory("RemoteSteps Info", :url => @url, :persist => true, :update => !@done) do
        init_job unless @url
        info = WorkflowRESTClient.get_json(File.join(@url, 'info'))
        info = WorkflowRESTClient.fix_hash(info)
        info[:status] = info[:status].to_sym if String === info[:status]
        info
      end
    end

    def status
      return :done if @done
      return nil unless url or started?
      return :streaming if @streaming
      begin
        status = info[:status]
        @done = true if status and status.to_sym == :done
        status
      rescue
        nil
      ensure
        @info = nil
      end
    end

    def started?
      @result != nil || @started || @streaming
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
      produce unless @started
      sleep 0.1 unless started?
      sleep 0.5 unless started?
      sleep 1 unless started?
      while not (done? or started?)
        sleep 1 
      end
    end

    #{{{ MANAGEMENT

    def init_job(cache_type = nil, other_params = {})
      cache_type = :asynchronous if cache_type.nil? and not @is_exec
      cache_type = :exec if cache_type.nil?
      get_streams
      @name ||= Persist.memory("RemoteSteps", :workflow => self, :task => task, :jobname => @name, :inputs => inputs, :cache_type => cache_type) do
        WorkflowRESTClient.post_jobname(File.join(base_url, task.to_s), inputs.merge(other_params).merge(:jobname => @name||@base_name, :_cache_type => cache_type))
      end
      if Open.remote? @name
        @url = @name
        @name = File.basename(@name)
      else
        @url = File.join(base_url, task.to_s, @name)
      end
      self
    end


    def fork(noload=false, semaphore=nil)
      init_job(:asynchronous)
    end

    def running?
      ! %w(done error aborted noinfo).include? status.to_s
    end

    def path
      if @url
        Misc.add_GET_param(@url, "_format", "raw")
      else
        [base_url, task, @base_name + '-' +  Misc.fingerprint(inputs)] * "/"
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
      init_job unless @url
      Log.debug{ "Joining RestClient: #{path}" }
      if IO === @result
        res = @result
        @result = nil
        Misc.consume_stream(res, true) 
      end

      if not (self.done? || self.aborted? || self.error?)
        self.info 
        return self if self.done? || self.aborted? || self.error?
        sleep 0.2 unless self.done? || self.aborted? || self.error?
        sleep 1 unless self.done? || self.aborted? || self.error?
        while not (self.done? || self.aborted? || self.error?)
          sleep 3
        end
      end

      self
    end

    def get
      params ||= {}
      params = params.merge(:_format => [:string, :boolean, :tsv, :annotations,:array].include?(result_type.to_sym) ? :raw : :json )
      Misc.insist 3, rand(2) + 1 do
        begin
          init_job if url.nil?
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
        json_text = if IO === res
                      res.read
                    else
                      res
                    end
        begin
          JSON.parse json_text
        rescue
          case
          when json_text =~ /^\d+$/
            json_text.to_i
          when json_text =~ /^\d+\.\d/
            json_text.to_f
          else
            raise $!
          end
        end
      end
    end

    def load
      params = {}
      join unless done? or streaming?
      raise get_exception if error? or aborted?
      load_res get
    end

    def exec_job
      res = _run_job(:exec)
      load_res res, result_type == :array ? :json : result_type
    end

    def _restart
      @done = nil
      @name = nil
      @started = nil
      @aborted = nil
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

    def _clean
      begin
        params = {:_update => :clean}
        WorkflowRESTClient.clean_url(url, params) if @url
        _restart
      rescue Exception
        Log.exception $!
      end
    end

    def clean
      init_job
      _clean
      self
    end

    def input_checks
      []
    end
  end
end

require 'rbbt/rest/client/run'
