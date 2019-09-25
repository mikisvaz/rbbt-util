module SSHClient
  def self.run(server, script)
    Log.debug "Run ssh script in #{server}:\n#{script}"
    CMD.cmd("ssh '#{server}' ruby ", :in => script).read.strip
  end

  def self.run_log(server, script)
    Log.debug "Run and monitor ssh script in #{server}:\n#{script}"
    CMD.cmd_log("ssh '#{server}' ruby ", :in => script)
  end

  def self.parse_url(url)
    m = url.match(/ssh:\/\/([^:]+):(.*)/)
    server = m.captures[0]
    path = m.captures[1]
    [server, path]
  end

  def self.path_script(path) 

    workflow, task, job, *rest = path.split("/")
    
    script =<<-EOF
require 'rbbt/workflow'
wf = Workflow.require_workflow "#{workflow}"
    EOF
    case task
    when nil
      script +=<<-EOF
task_info = {}
wf.tasks.keys.each do |task|
  task_info[task] = wf.task_info(task)
end
res = task_info
      EOF
    when 'documentation'
      script +=<<-EOF
res = documentation = wf.documentation
      EOF
    else
      if job.nil?
        script +=<<-EOF
task = '#{task}'
res = task_info = wf.task_info(task)
        EOF
      else
        case rest.first
        when nil
          script +=<<-EOF
task = '#{task}'
jobname = '#{job}'
res = job = wf.fast_load_id(File.join(task, jobname))
          EOF
        when "info"
          script +=<<-EOF
task = '#{task}'
jobname = '#{job}'
job = wf.fast_load_id(File.join(task, jobname))
res = job_info = job.info
          EOF
        else
          raise "Unkown path: #{[path, rest].inspect}"
        end
      end
    end
  end

  def self.get_json(url, params)
    server, path = parse_url(url)
    script = path_script(path)

    script +=<<-EOF
puts res.to_json
    EOF

    JSON.parse(self.run(server, script))
  end

  def self.get_raw(url, params)
    server, path = parse_url(url)
    script = path_script(path)

    script +=<<-EOF
puts res
    EOF

    self.run(server, script)
  end

  def self.post_job(url, inputs, input_types, jobname = nil)
    server, path = parse_url(url)
    script = path_script(path)
    
    id = "inputs-" << rand(100000).to_s
    CMD.cmd("ssh '#{server}' mkdir -p .rbbt/tmp/tmp-ssh_job_inputs/ ", :in => script).read
    TmpFile.with_file do |dir|
      if Step.save_inputs(inputs, input_types, dir)
        CMD.cmd("scp -r '#{dir}' .rbbt/tmp/tmp-ssh_job_inputs/#{id}")
      end
    end

    script +=<<-EOF
jobname = #{jobname.nil? ? 'nil' : "'#{jobname}'"}
path = '.rbbt/tmp/tmp-ssh_job_inputs/#{id}'
job_inputs = Workflow.load_inputs(path, task_info[:inputs], task_info[:input_types])
job = wf.job(task, jobname, job_inputs)
Log.severity = 10
job.fork
puts job.name
    EOF
    self.run(server, script)
  end

  def self.run_job(url, inputs, input_types, jobname = nil)
    server, path = parse_url(url)
    script = path_script(path)
    
    id = "inputs-" << rand(100000).to_s
    CMD.cmd("ssh '#{server}' mkdir -p .rbbt/tmp/tmp-ssh_job_inputs/ ", :in => script).read
    TmpFile.with_file do |dir|
      if Step.save_inputs(inputs, input_types, dir)
        CMD.cmd("scp -r '#{dir}' .rbbt/tmp/tmp-ssh_job_inputs/#{id}")
      end
    end

    script +=<<-EOF
jobname = #{jobname.nil? ? 'nil' : "'#{jobname}'"}
path = '.rbbt/tmp/tmp-ssh_job_inputs/#{id}'
job_inputs = Workflow.load_inputs(path, task_info[:inputs], task_info[:input_types])
job = wf.job(task, jobname, job_inputs)
job.run
    EOF
    self.run_log(server, script)
  end

  def self.clean(url)
    server, path = parse_url(url)
    script = path_script(path)
    
    script +=<<-EOF
job.clean
    EOF
    self.run(server, script)
  end

end

module WorkflowSSHClient
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
        SSHClient.get_json(url, :params => params)
      end
    end

    begin
      JSON.parse(res)
    rescue
      res
    end
  end


  def self.post_jobname(url, inputs, input_types)
    SSHClient.post_job(url, inputs, input_types)
  end

  def init_job(cache_type = nil, other_params = {})
    cache_type = :asynchronous if cache_type.nil? and not @is_exec
    cache_type = :exec if cache_type.nil?
    @last_info_time = nil
    @done = false
    get_streams
    @name ||= Persist.memory("RemoteSteps", :workflow => self, :task => task, :jobname => @name, :inputs => inputs, :cache_type => cache_type) do
      Misc.insist do
        #@adaptor.post_jobname(File.join(base_url, task.to_s), inputs.merge(other_params).merge(:jobname => @name||@base_name, :_cache_type => cache_type))
        input_types = {}
        @adaptor.post_jobname(File.join(base_url, task.to_s), inputs, input_types)
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
    server, path = SSHClient.parse_url(url)
    "ssh://" + server + ":" + info[:path]
  end

  def run(*args)
    input_types = {}
    SSHClient.run_job(File.join(base_url, task.to_s), inputs, input_types)
  end


  def clean
    init_job
    SSHClient.clean(@url) if done?
    _restart
  end

end
