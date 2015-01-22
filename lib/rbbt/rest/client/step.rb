class WorkflowRESTClient
  class RemoteStep < Step

    attr_accessor :url, :base_url, :task, :base_name, :inputs, :result_type, :result_description, :is_exec

    def self.get_streams(inputs)
      inputs.each do |k,v|
        if Step === v
          stream = v.get_stream
          inputs[k] = stream || v.load
        end
      end
    end
    def initialize(base_url, task = nil, base_name = nil, inputs = nil, result_type = nil, result_description = nil, is_exec = false)
      @base_url, @task, @base_name, @inputs, @result_type, @result_description, @is_exec = base_url, task, base_name, inputs, result_type, result_description, is_exec
      @mutex = Mutex.new
      RemoteStep.get_streams @inputs
    end

    def name
      return nil if @is_exec
      (Array === @url ? @url.first : @url).split("/").last
    end

    def task_name
      (Array === @url ? @url.first : @url).split("/")[-2]
    end

    def info(check_lock=false)
      @info ||= begin
                  init_job unless url
                  info = WorkflowRESTClient.get_json(File.join(url, 'info'))
                  info = WorkflowRESTClient.fix_hash(info)
                  info[:status] = info[:status].to_sym if String === info[:status]
                  info
                end
    end
    
    def status
      begin
        info[:status]
      ensure
        @info = nil
      end
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

    #{{{ MANAGEMENT
    
    def init_job(cache_type = :asynchronous)
      @name ||= Persist.memory("RemoteSteps", :jobname => @name, :inputs => inputs) do
        WorkflowRESTClient.post_jobname(File.join(base_url, task.to_s), inputs.merge(:jobname => @name||@base_name, :_cache_type => cache_type))
      end
      @url = File.join(base_url, task.to_s, @name)
      nil
    end

    def load_res(res, result_type = nil)
      result_type ||= self.result_type
      case result_type
      when :string
        res
      when :boolean
        res == "true"
      when :tsv
        TSV.open(StringIO.new(res))
      when :annotations
        Annotated.load_tsv(TSV.open(StringIO.new(res)))
      when :array
        res.split("\n")
      else
        JSON.parse res
      end
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

    def load
      params = {}
      load_res get
    end
    
    def exec_job
      res = WorkflowRESTClient.capture_exception do
        RestClient.post(URI.encode(File.join(base_url, task.to_s)), inputs.merge(:_cache_type => :exec, :_format => [:string, :boolean, :tsv, :annotations].include?(result_type) ? :raw : :json))
      end
      load_res res, result_type == :array ? :json : result_type
    end

    def fork
      init_job(:asynchronous)
    end

    def running?
      ! %w(done error aborted).include? status.to_s
    end

    def path
      url
    end

    def run(noload = false)
      @mutex.synchronize do
        @result ||= begin
                      if @is_exec
                        exec_job 
                      else
                        init_job(:synchronous) 
                        self.load
                      end
                    end
      end
      noload ? path : @result
    end

    def exec(*args)
      exec_job
    end

    def join
      return if self.done?
      self.load
      self
    end

    def recursive_clean
      begin
        inputs[:_update] = :recursive_clean
      rescue Exception
      end
      self
    end

    def clean
      begin
        inputs[:_update] = :clean
      rescue Exception
      end
      self
    end
  end
end
