register Sinatra::RbbtRESTWorkflow

Rbbt.etc.workflows.find.read.split("\n").each do |workflow|
  add = true

  if workflow =~ /(.*)\*$/
    workflow = $1
    add = :priority
  end

  if workflow =~ /(.*)\-$/
    workflow = $1
    add = false
  end


  next if workflow.empty?
  Workflow.require_workflow workflow
  add_workflow Kernel.const_get(workflow), add
end if Rbbt.etc.workflows.find.exists?
