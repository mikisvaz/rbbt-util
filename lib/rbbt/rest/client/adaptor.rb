class WorkflowRESTClient

  def workflow_description
    WorkflowRESTClient.get_raw(File.join(url, 'description'))
  end

  def documentation
    @documention ||= IndiferentHash.setup(WorkflowRESTClient.get_json(File.join(url, "documentation"),{}))
  end

  def task_info(task)
    @task_info ||= {}
    @task_info[task]
    
    if @task_info[task].nil?
      task_info = WorkflowRESTClient.get_json(File.join(url, task.to_s, 'info'))
      task_info = WorkflowRESTClient.fix_hash(task_info)

      task_info[:result_type] = task_info[:result_type].to_sym
      task_info[:export] = task_info[:export].to_sym
      task_info[:input_types] = WorkflowRESTClient.fix_hash(task_info[:input_types], true)
      task_info[:inputs] = task_info[:inputs].collect{|input| input.to_sym }

      @task_info[task] = task_info
    end
    @task_info[task]
  end

  def exported_tasks
    (@asynchronous_exports  + @synchronous_exports + @exec_exports).compact.flatten
  end

  def tasks
    @tasks ||= Hash.new do |hash,task_name| 
      info = task_info(task_name)
      task = Task.setup info do |*args|
        raise "This is a remote task" 
      end
      task.name = task_name.to_sym
      hash[task_name] = task
    end
  end

  def load_tasks
    exported_tasks.each{|name| tasks[name]}
    nil
  end

  def task_dependencies
    @task_dependencies ||= Hash.new do |hash,task| 
      hash[task] = if exported_tasks.include? task
        WorkflowRESTClient.get_json(File.join(url, task.to_s, 'dependencies'))
      else
        []
      end
    end
  end

  def init_remote_tasks
    task_exports = WorkflowRESTClient.get_json(url)
    @asynchronous_exports = task_exports["asynchronous"].collect{|task| task.to_sym }
    @synchronous_exports = task_exports["synchronous"].collect{|task| task.to_sym }
    @exec_exports = task_exports["exec"].collect{|task| task.to_sym }
    nil
  end
end
