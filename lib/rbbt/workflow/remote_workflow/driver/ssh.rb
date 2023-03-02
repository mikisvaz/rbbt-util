class RemoteWorkflow
  RBBT_DEBUG_REMOTE_JSON = ENV["RBBT_DEBUG_REMOTE_JSON"] == 'true'

  module SSH
    #def self.run(server, script)
    #  Log.debug "Run ssh script in #{server}:\n#{script}"
    #  CMD.cmd("ssh '#{server}' 'shopt -s expand_aliases; bash -l -i -c \"ruby\"' ", :in => script, :log => true).read
    #end

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

      workflow_name = begin
                        workflow = Kernel.const_get(workflow) if String === workflow
                        workflow.respond_to?(:complete_name) ? workflow.complete_name : workflow
                      rescue
                        workflow
                      end

      script =<<-EOF
require 'rbbt/workflow'
wf = Workflow.require_workflow "#{workflow_name}"
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

    def self.get_json(url, params = {})
      server, path = parse_url(url)
      script = path_script(path)

      script +=<<-EOF
STDOUT.write res.to_json
      EOF

      json = Misc.ssh_run(server, script)
      Log.debug "JSON (#{ url }): #{json}" if RBBT_DEBUG_REMOTE_JSON
      JSON.parse(json)
    end

    def self.get_raw(url, params)
      server, path = parse_url(url)
      script = path_script(path)

      script +=<<-EOF
STDOUT.write res
      EOF

      Misc.ssh_run(server, script)
    end

    def self.post_job(url, inputs_id, jobname = nil)
      server, path = parse_url(url)

      script = path_script(path)
      script += job_script(inputs_id, jobname)
      script +=<<-EOF
job.init_info
STDOUT.write job.path
      EOF
      Misc.ssh_run(server, script)
    end

    def self.run_job(url, input_id, jobname = nil)
      server, path = parse_url(url)

      script = path_script(path)
      script += job_script(input_id, jobname)
      script +=<<-EOF
ENV["RBBT_UPDATE"]="#{(ENV["RBBT_UPDATE"] || false).to_s}"
job.clean if job.error? and job.recoverable_error?
job.run unless job.done? || job.error?
STDOUT.write job.path
      EOF
      Misc.ssh_run(server, script)
    end

    def self.run_slurm_job(url, input_id, jobname = nil, slurm_options = {})
      server, path = parse_url(url)

      script = path_script(path)
      script += job_script(input_id, jobname)
      script +=<<-EOF
require 'rbbt/hpc'
HPC::BATCH_MODULE = HPC.batch_system "SLURM"
slurm_options = JSON.parse(%q(#{slurm_options.to_json}))
job.clean if job.error? and job.recoverable_error?
HPC::BATCH_MODULE.run_job(job, slurm_options) unless job.done? || job.error?
STDOUT.write job.path
      EOF
      Misc.ssh_run(server, script)
    end

    def self.orchestrate_slurm_job(url, input_id, jobname = nil, slurm_options = {})
      server, path = parse_url(url)

      script = path_script(path)
      script += job_script(input_id, jobname)
      script +=<<-EOF
require 'rbbt/hpc'
HPC::BATCH_MODULE = HPC.batch_system "SLURM"
slurm_options = JSON.parse(%q(#{slurm_options.to_json}))
job.clean if job.error? and job.recoverable_error?
HPC::BATCH_MODULE.orchestrate_job(job, slurm_options) unless job.done? || job.error?
STDOUT.write job.path
      EOF
      Misc.ssh_run(server, script)
    end

    def self.clean(url, input_id, jobname = nil)
      server, path = parse_url(url)

      script = path_script(path)
      script +=<<-EOF
job.clean
      EOF
      Misc.ssh_run(server, script)
    end

    def self.upload_inputs(server, inputs, input_types, input_id)
      TmpFile.with_file do |dir|
        if Step.save_inputs(inputs, input_types, dir)
          # Dir.glob(File.join(dir, "*.as_step")).each do |file|
          #   Log.medium "Migrating Step input #{file} #{ server }" 
          #   path = Open.read(file).strip
          #   new = Step.migrate(path, :user, :target => server)
          #   Open.write(file, new)
          # end

          paths = Dir.glob(File.join(dir, "*.as_step")).collect{|f| Open.read(f).strip }
          new = Step.migrate(paths, :user, :target => server)
          paths.zip(new).each{|file,new| Open.write(file, new) }

          CMD.cmd_log("ssh '#{server}' mkdir -p .rbbt/tmp/tmp-ssh_job_inputs/; scp -r '#{dir}' #{server}:.rbbt/tmp/tmp-ssh_job_inputs/#{input_id}")
        end
      end
    end

    #def self.relay_old(workflow, task, jobname, inputs, server, options = {})
    #  options = Misc.add_defaults options, :search_path => 'user'
    #  search_path = options[:search_path]

    #  job = workflow.job(task, jobname, inputs)

    #  job.dependencies.each do |dep| 
    #    dep.produce 
    #  end

    #  override_dependencies = job.dependencies.collect{|dep| [dep.workflow.to_s, dep.task_name.to_s] * "#" << "=" << Rbbt.identify(dep.path)}

    #  job.dependencies.each do |dep| 
    #    Step.migrate(dep.path, search_path, :target => server)
    #  end

    #  remote = RemoteWorkflow.new("ssh://#{server}:#{workflow.to_s}", "#{workflow.to_s}")
    #  rjob = remote.job(task, jobname, {})
    #  rjob.override_dependencies = override_dependencies
    #  rjob.run
    #end

    def self.upload_dependencies(job, server, search_path = 'user', produce_dependencies = false)
      server, path = parse_url(server) if server =~ /^ssh:\/\//
      job.dependencies.each do |dep|
        Log.medium "Producing #{dep.workflow}:#{dep.short_path} dependency for #{job.workflow}:#{job.short_path}"
        dep.produce
      end if produce_dependencies

      job.input_dependencies.each do |dep|
        Log.medium "Producing #{dep.workflow}:#{dep.short_path} dependency for #{job.workflow}:#{job.short_path}"
        dep.produce
      end if produce_dependencies

      migrate_dependencies = job.rec_dependencies.select{|d| d.done? }.collect{|d| d.path }
      Log.medium "Migrating #{migrate_dependencies.length} dependencies to #{ server }" 
      Step.migrate(migrate_dependencies, search_path, :target => server) if migrate_dependencies.any?
    end

    def self.missing_dep_inputs(job)
      inputs = job.inputs.to_hash.slice(*job.real_inputs.map{|i| i.to_s})
      job.dependencies.each do |dep|
        next if dep.done?
        iif [dep, dep.inputs, dep.real_inputs]
        inputs = dep.inputs.to_hash.slice(*dep.real_inputs.map{|i| i.to_s}).merge(inputs)
        inputs = missing_dep_inputs(dep).merge(inputs)
      end
      inputs
    end

    def self.relay_job(job, server, options = {})
      migrate, produce, produce_dependencies, search_path, run_type, slurm_options = Misc.process_options options.dup,
        :migrate, :produce, :produce_dependencies, :search_path, :run_type, :slurm_options

      search_path ||= 'user'

      produce = true if migrate

      workflow_name = job.workflow.to_s
      remote_workflow = RemoteWorkflow.new("ssh://#{server}:#{workflow_name}", "#{workflow_name}")
      inputs = job.recursive_inputs.to_hash.slice(*job.real_inputs.map{|i| i.to_s})
      Log.medium "Relaying dependency #{job.workflow}:#{job.short_path} to #{server} (#{inputs.keys * ", "})"

      upload_dependencies(job, server, search_path, options[:produce_dependencies])
      rjob = remote_workflow.job(job.task_name.to_s, job.clean_name, inputs)

      override_dependencies = job.rec_dependencies.select{|dep| dep.done? }.collect{|dep| [dep.workflow.to_s, dep.task_name.to_s] * "#" << "=" << Rbbt.identify(dep.path)}
      rjob.override_dependencies = override_dependencies

      rjob.run_type = run_type
      rjob.slurm_options = slurm_options || {}

      if options[:migrate]
        rjob.produce
        Step.migrate(Rbbt.identify(job.path), 'user', :source => server) 
      end

      rjob
    end

    def self.relay(workflow, task, jobname, inputs, server, options = {})
      job = workflow.job(task, jobname, inputs)
      relay_job(job, server, options)
    end

    def workflow_description
      RemoteWorkflow::SSH.get_raw(File.join(url, 'description'))
    end

    def documentation
      @documention ||= IndiferentHash.setup(RemoteWorkflow::SSH.get_json(File.join(url, "documentation")))
      @documention
    end

    def task_info(task)
      @task_info ||= IndiferentHash.setup({})

      if @task_info[task].nil?
        task_info = RemoteWorkflow::SSH.get_json(File.join(@base_url || @url, task.to_s))
        task_info = RemoteWorkflow::SSH.fix_hash(task_info)

        task_info[:result_type] = task_info[:result_type].to_sym if task_info[:result_type]
        task_info[:export] = task_info[:export].to_sym if task_info[:export]
        task_info[:input_types] = RemoteWorkflow::SSH.fix_hash(task_info[:input_types], true)
        task_info[:inputs] = task_info[:inputs].collect{|input| input.to_sym }

        @task_info[task] = IndiferentHash.setup(task_info)
      end

      IndiferentHash.setup(@task_info[task])
    end

    def tasks
      @tasks ||= Hash.new do |hash,task_name| 
        raise Workflow::TaskNotFoundException, "Task #{task_name} not found in workflow #{self.to_s}" unless @task_info.include?(task_name)
        info = @task_info[task_name]
        task = Task.setup info do |*args|
          raise "This is a remote task" 
        end
        task.name = task_name.to_sym
        hash[task_name] = task
      end
    end

    def task_dependencies
      @task_dependencies ||= Hash.new do |hash,task| 
        hash[task] = if exported_tasks.include? task
                       RemoteWorkflow::SSH.get_json(File.join(url, task.to_s, 'dependencies'))
                     else
                       []
                     end
      end
    end

    def init_remote_tasks
      @task_info = IndiferentHash.setup(RemoteWorkflow::SSH.get_json(url))
      @exec_exports = @stream_exports = @synchronous_exports = []
      @asynchronous_exports = @task_info.keys
    end
  end
end
