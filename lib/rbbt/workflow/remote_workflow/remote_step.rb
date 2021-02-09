require 'rbbt/workflow'

class RemoteStep < Step

  attr_accessor :url, :base_url, :task, :base_name, :inputs, :input_types, :result_type, :result_description, :is_exec, :is_stream, :stream_input, :started

  def initialize(base_url, task = nil, base_name = nil, inputs = nil, input_types = nil, result_type = nil, result_description = nil, is_exec = false, is_stream = false, stream_input = nil)
    @base_url, @task, @base_name, @inputs, @input_types, @result_type, @result_description, @is_exec, @is_stream, @stream_input = base_url, task, base_name, inputs, input_types, result_type, result_description, is_exec, is_stream, stream_input
    @base_url = "http://" << @base_url unless @base_url =~ /^[a-z]+:\/\//
    @mutex = Mutex.new
    rest = base_url.include?('ssh:') ? false : true

    if rest
      @adaptor = RemoteWorkflow::REST
      self.extend RemoteStep::REST
    else
      @adaptor = RemoteWorkflow::SSH
      self.extend RemoteStep::SSH
    end

  end

  def clean_name
    @base_name
  end

  def cache_file
    begin
      digest = Misc.obj2digest([base_url, task.to_s, base_name, inputs])
      Rbbt.var.cache.REST[task.to_s][[clean_name, digest].compact * "."].find
    rescue
      Log.exception $!
      raise $!
    end
  end

  def cache_files
    Dir.glob(cache_file + '.*')
  end

  def run(no_load = false)
    no_load = @is_stream ? :stream : true if no_load

    @result ||= @mutex.synchronize do
      begin
        if @is_exec
          exec(no_load)
        elsif no_load == :stream
          _run_job(:stream)
        elsif no_load
          init_job 
          nil
        else
          if ! done?
            init_job 
            produce
            join
          end
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
    @inputs = RemoteStep.get_streams @inputs, @stream_input
    @inputs_done = true
    @inputs
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
    return {:status => :waiting } unless started?
    @done = @info && @info[:status] && (@info[:status].to_sym == :done || @info[:status].to_sym == :error)

    if !@done && (@last_info_time.nil? || (Time.now - @last_info_time) > 0.5)
      update = true 
    else
      update = false
    end

    @info = Persist.memory("RemoteSteps Info", :url => @url, :persist => true, :update => update) do
      @last_info_time = Time.now
      init_job unless @url
      info = begin
               @adaptor.get_json(File.join(@url, 'info'))
             rescue
               {:status => :noinfo}
             end
      info = RemoteWorkflow.fix_hash(info)
      info[:status] = info[:status].to_sym if String === info[:status]
      info
    end

    @info
  end

  def status
    return :done if @done
    return nil unless url or started?
    #return :streaming if @streaming 
    begin
      status = info[:status]
      @done = true if status and status.to_sym == :done
      status
    rescue
      Log.exception $!
      nil
    ensure
      @info = nil
    end
  end

  def started?
    @result != nil || @started || @streaming
  end

  def done?
    return true if cache_files.any?
    self.init_job unless @url
    @done || status.to_s == 'done' || status.to_s == 'noinfo'
  end

  def files
    @adaptor.get_json(File.join(url, 'files'))
  end

  def file(file)
    @adaptor.get_raw(File.join(url, 'file', file.to_s))
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


  def path
    if @url
      Misc.add_GET_param(@url, "_format", "raw")
    elsif @base_name
      [base_url, task, @base_name + '-' +  Misc.fingerprint(inputs)] * "/"
    else
      nil
    end
  end

  def fork(noload=false, semaphore=nil)
    init_job(:asynchronous)
  end

  def running?
    ! %w(done error aborted noinfo).include? status.to_s
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
    return true if cache_files.any?
    init_job unless @url
    produce unless @started
    Log.debug{ "Joining RemoteStep: #{path}" }

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

  def load_res(res, result_type = nil)

    stream = true if res.respond_to? :read
    join unless stream
    result_type ||= self.result_type

    case result_type.to_sym
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

  def workflow_short_path
    init_job unless @url
    [@base_url, @task, @name] * "/"
  end

  def short_path
    init_job unless @url
    [@task, @name] * "/"
  end

  def input_checks
    []
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
    @info = nil
  end

  def init_info(*args)
    i = {:status => :waiting, :pid => Process.pid, :path => path}
    i[:dependencies] = dependencies.collect{|dep| [dep.task_name, dep.name, dep.path]} if dependencies
  end
end

require 'rbbt/workflow/remote_workflow/remote_step/rest'
require 'rbbt/workflow/remote_workflow/remote_step/ssh'
