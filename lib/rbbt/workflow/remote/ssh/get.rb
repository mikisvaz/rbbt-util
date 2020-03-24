module WorkflowSSHClient
  attr_accessor :override_dependencies

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

  def self.get_json(url, params = {})
    Log.debug{ "SSHClient get_json: #{ url } - #{Misc.fingerprint params }" }
    params = params.merge({ :_format => 'json' })
    params = fix_params params

    res = capture_exception do
      Misc.insist(2, 0.5) do
        SSHDriver.get_json(url, :params => params)
      end
    end

    begin
      JSON.parse(res)
    rescue
      res
    end
  end

  def self.upload_inputs(server, inputs, input_types, input_id)
    TmpFile.with_file do |dir|
      if Step.save_inputs(inputs, input_types, dir)
        CMD.cmd("ssh '#{server}' mkdir -p .rbbt/tmp/tmp-ssh_job_inputs/; scp -r '#{dir}' #{server}:.rbbt/tmp/tmp-ssh_job_inputs/#{input_id}")
      end
    end
  end

  #{{{ RUN
  

  def init_job(cache_type = nil, other_params = {})
    cache_type = :asynchronous if cache_type.nil? and not @is_exec
    cache_type = :exec if cache_type.nil?
    @last_info_time = nil
    @done = false
    @server, @server_path = SSHDriver.parse_url base_url
    @input_id ||= "inputs-" << rand(100000).to_s
    @input_types = task_info(task)[:input_types]

    if override_dependencies

      if override_dependencies && override_dependencies.any?
        override_dependencies.each do |od|
          name, _sep, value = od.partition("=")
          inputs[name] = value
        end
      end
      iii inputs

      WorkflowSSHClient.upload_inputs(@server, inputs, @input_types, @input_id)
    else
      WorkflowSSHClient.upload_inputs(@server, inputs, @input_types, @input_id)
    end

    @name ||= Persist.memory("RemoteSteps", :workflow => self, :task => task, :jobname => @name, :inputs => inputs, :cache_type => cache_type) do
      Misc.insist do
        input_types = {}
        SSHDriver.post_job(File.join(base_url, task.to_s), @input_id, @base_name)
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

  def path
    @server, @server_path = SSHDriver.parse_url @base_url
    "ssh://" + @server + ":" + @remote_path
  end

  def produce(*args)
    input_types = {}
    init_job
    @remote_path = SSHDriver.run_job(File.join(base_url, task.to_s), @input_id, @base_name)
    while ! done?
      sleep 1
    end
  end

  def load
    load_res Open.open(path)
  end

  def run(*args)
    produce(*args)
    self.load unless args.first
  end

  def clean
    init_job
    SSHDriver.clean(@url, @input_id, @base_name) if done?
    _restart
  end

  def self.relay(workflow, task, jobname, inputs, server, options = {})
    options = Misc.add_defaults options, :search_path => 'user'
    search_path = options[:search_path]

    job = workflow.job(task, jobname, inputs)

    job.dependencies.each do |dep| 
      dep.produce 
    end

    override_dependencies = job.dependencies.collect{|dep| [dep.workflow.to_s, dep.task_name.to_s] * "#" << "=" << Rbbt.identify(dep.path)}

    job.dependencies.each do |dep| 
      Step.migrate(dep.path, search_path, :target => server)
    end

    remote = WorkflowRemoteClient.new("ssh://#{server}:#{workflow.to_s}", "#{workflow.to_s}")
    rjob = remote.job(task, jobname, {})
    rjob.override_dependencies = override_dependencies
    rjob.run
  end

end
