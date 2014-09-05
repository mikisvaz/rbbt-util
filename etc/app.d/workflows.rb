register Sinatra::RbbtRESTWorkflow

Rbbt.etc.workflows.find.read.split("\n").each do |workflow|
  next if workflow.empty?
  Workflow.require_workflow workflow
  add_workflow Kernel.const_get(workflow), true
end if Rbbt.etc.workflows.find.exists?
