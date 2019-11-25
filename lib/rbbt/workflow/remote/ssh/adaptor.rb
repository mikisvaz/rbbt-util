require 'rbbt/workflow/remote/ssh/driver'

module WorkflowSSHClient
  def workflow_description
    WorkflowSSHClient.get_raw(File.join(url, 'description'))
  end

  def documentation
    @documention ||= IndiferentHash.setup(WorkflowSSHClient.get_json(File.join(url, "documentation")))
    @documention
  end

  def task_info(task)
    @task_info ||= IndiferentHash.setup({})
    
    if @task_info[task].nil?
      task_info = WorkflowSSHClient.get_json(File.join(@base_url, task.to_s))
      task_info = WorkflowSSHClient.fix_hash(task_info)

      task_info[:result_type] = task_info[:result_type].to_sym if task_info[:result_type]
      task_info[:export] = task_info[:export].to_sym if task_info[:export]
      task_info[:input_types] = WorkflowSSHClient.fix_hash(task_info[:input_types], true)
      task_info[:inputs] = task_info[:inputs].collect{|input| input.to_sym }

      @task_info[task] = IndiferentHash.setup(task_info)
    end

    IndiferentHash.setup(@task_info[task])
  end

  def tasks
    @tasks ||= Hash.new do |hash,task_name| 
      info = @task_info[task_name]
      task = Task.setup info do |*args|
        raise "This is a remote task" 
      end
      task.name = task_name.to_sym
      hash[task_name] = task
    end
  end

  def load_tasks
    @task_info.keys.each{|name| tasks[name]}
  end

  def task_dependencies
    @task_dependencies ||= Hash.new do |hash,task| 
      hash[task] = if exported_tasks.include? task
        WorkflowSSHClient.get_json(File.join(url, task.to_s, 'dependencies'))
      else
        []
      end
    end
  end

  def init_remote_tasks
    @task_info = IndiferentHash.setup(WorkflowSSHClient.get_json(url))
    @exec_exports = @stream_exports = @synchronous_exports = []
    @asynchronous_exports = @task_info.keys
  end
end
