module SSHDriver
  def self.run(server, script)
    Log.debug "Run ssh script in #{server}:\n#{script}"
    CMD.cmd("ssh '#{server}' 'shopt -s expand_aliases; bash -ic \"ruby\"' ", :in => script, :log => true).read
  end

  #def self.run_log(server, script)
  #  Log.debug "Run and monitor ssh script in #{server}:\n#{script}"
  #  CMD.cmd("ssh '#{server}' 'shopt -s expand_aliases; bash -ic \"ruby\"' ", :in => script, :log => true)
  #end

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

  def self.job_script(inputs_id, jobname = nil)
    script =<<-EOF
jobname = #{jobname.nil? ? 'nil' : "'#{jobname}'"}
path = File.join(ENV["HOME"], '.rbbt/tmp/tmp-ssh_job_inputs/#{inputs_id}')
job_inputs = Workflow.load_inputs(path, task_info[:inputs], task_info[:input_types])
job = wf.job(task, jobname, job_inputs)
    EOF
    script
  end

  def self.get_json(url, params)
    server, path = parse_url(url)
    script = path_script(path)

    script +=<<-EOF
STDOUT.write res.to_json
    EOF

    JSON.parse(self.run(server, script))
  end

  def self.get_raw(url, params)
    server, path = parse_url(url)
    script = path_script(path)

    script +=<<-EOF
STDOUT.write res
    EOF

    self.run(server, script)
  end

  def self.post_job(url, inputs_id, jobname = nil)
    server, path = parse_url(url)

    script = path_script(path)
    script += job_script(inputs_id, jobname)
    script +=<<-EOF
job.init_info
STDOUT.write job.name
    EOF
    @name = self.run(server, script)
  end

  def self.run_job(url, input_id, jobname = nil)
    server, path = parse_url(url)
    
    script = path_script(path)
    script += job_script(input_id, jobname)
    script +=<<-EOF
job.produce
STDOUT.write job.path
    EOF
    self.run(server, script)
  end

  def self.run_slurm_job(url, input_id, jobname = nil)
    server, path = parse_url(url)
    
    script = path_script(path)
    script += job_script(input_id, jobname)
    script +=<<-EOF
job.produce
STDOUT.write job.path
    EOF
    self.run(server, script)
  end

  def self.clean(url, input_id, jobname = nil)
    server, path = parse_url(url)
    
    script = path_script(path)
    script +=<<-EOF
job.clean
    EOF
    self.run(server, script)
  end

end
