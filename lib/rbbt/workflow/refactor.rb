require_relative 'refactor/inputs'
require_relative 'refactor/entity'

class Step
  alias get_stream stream
  alias old_exec exec

  def exec(noload = false)
    old_exec
  end

  def self.md5_file(path)
    path.nil? ? nil : path + '.md5'
  end

  def md5_file
    Step.md5_file(path)
  end

  alias real_inputs non_default_inputs

  def reset_info(info = {})
    if ENV["BATCH_SYSTEM"]
      info = info.dup
      info[:batch_system] = ENV["BATCH_SYSTEM"]
      info[:batch_job] = ENV["BATCH_JOB_ID"]
    end
    save_info(info)
  end
end

module Workflow
  alias workdir= directory=
  
  def resumable
    Log.warn "RESUMABLE MOCKED"
  end

  DEFAULT_NAME = Task::DEFAULT_NAME

end

module ComputeDependency
  attr_accessor :compute
  def self.setup(dep, value)
    dep.extend ComputeDependency
    dep.compute = value
  end

  def canfail?
    compute == :canfail || (Array === compute && compute.include?(:canfail))
  end
end

class Step

  def soft_grace
    sleep 1 until Open.exists?(info_file)
  end
end

Rbbt.relay_module_method Workflow, :load_step, Step, :load
Rbbt.relay_module_method Workflow, :fast_load_step, Step, :load

module Workflow
  attr_accessor :remote_tasks
  def remote_tasks
    @remote_tasks ||= {}
  end
  def task_for(path)
    parts = path.split("/")
    if parts.include?(self.to_s)
      parts[parts.index(self.to_s) + 1]
    else
      parts[-2]
    end
  end

  def fast_load_id(id)
    path = if Path === directory
             directory[id].find
           else
             File.join(directory, id)
           end
    task = task_for path
    return remote_tasks[task].load_id(id) if remote_tasks && remote_tasks.include?(task)
    return Workflow.fast_load_step path
  end

  alias load_id fast_load_id

  class << self
    alias original_require_workflow require_workflow
  
    def require_remote_workflow(wf_name, url)
      require 'rbbt/workflow/remote_workflow'
      eval "Object::#{wf_name.split("+").first} = RemoteWorkflow.new '#{ url }', '#{wf_name}'"
    end

    def require_workflow(wf_name, force_local = true)
      if Open.remote?(wf_name) or Open.ssh?(wf_name)
        url = wf_name

        if Open.ssh?(wf_name)
          wf_name = File.basename(url.split(":").last)
        else
          wf_name = File.basename(url)
        end

        begin
          return require_remote_workflow(wf_name, url)
        ensure
          Log.debug{"Workflow #{ wf_name } loaded remotely: #{ url }"}
        end
      end

      original_require_workflow(wf_name)
    end
  end
end

module Workflow
  class << self
    def workflow_dir
      @workflow_dir || 
        ENV["RBBT_WORKFLOW_DIR"] || 
        begin 
          workflow_dir_config = Path.setup("etc/workflow_dir")
          if workflow_dir_config.exists?
            Path.setup(workflow_dir_config.read.strip)
          else
            Path.setup('workflows').find(:user)
          end
        end
    end

    def workflow_repo
      @workflow_repo || 
        ENV["RBBT_WORKFLOW_REPO"] || 
        begin 
          workflow_repo_config = Path.setup("etc/workflow_repo")
          if workflow_repo_config.exists?
            workflow_repo_config.read.strip
          else
            'https://github.com/Rbbt-Workflows/'
          end
        end
    end
  end
end
