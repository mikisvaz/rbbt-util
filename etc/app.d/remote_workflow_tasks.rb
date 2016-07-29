
Rbbt.etc.remote_tasks.open do |io|
  remote_workflow_tasks = YAML.load(io.read)
  Workflow.process_remote_tasks(remote_workflow_tasks)
end if Rbbt.etc.remote_tasks.exists?
