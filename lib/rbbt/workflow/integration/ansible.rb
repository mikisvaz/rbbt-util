require_relative 'ansible/workflow'
require 'rbbt/workflow/usage'

module Ansible
  def self.play(playbook, inventory = nil, verbose = false)
    inventory = Rbbt.etc.ansible_inventory.find
    Log.with_severity 0 do
      TmpFile.with_file do |tmp|
        if Hash === playbook
          Open.write(tmp, [playbook].to_yaml)
          playbook = tmp
        end
        if verbose
          CMD.cmd_log("ansible-playbook -i #{inventory} -v #{playbook}")
        else
          CMD.cmd_log("ansible-playbook -i #{inventory} #{playbook}")
        end
      end
    end
  end

  def self.clean_symbols(hash)
    new = {}
    hash.each do |key,value|
      key = key.to_s
      value = case value
              when Symbol
                value.to_s
              when Hash
                self.clean_symbols(value)
              else
                value
              end
      new[key] = value
    end
    new
  end

  def self.workflow2playbook(workflow, task, options = {})
    job_options = workflow.get_SOPT(workflow.tasks[task])

    tasks = workflow.job(task, nil, job_options).exec

    hosts = options[:hosts] || 'localhost'

    clean_tasks = tasks.collect{|task| self.clean_symbols task }
    {"hosts" => hosts, "tasks" => clean_tasks}
  end

  def self.playbook(file, task = nil, options = {})
    task = task.to_sym if String === task

    workflow = Workflow === file ? file : Workflow.require_workflow(file)
    task = workflow.tasks.keys.last if workflow.tasks[task].nil?
    workflow2playbook workflow, task, options
  end
end
