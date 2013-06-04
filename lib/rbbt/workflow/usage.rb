require 'rbbt/util/simpleopt'

module Task
  def doc(deps = nil)
    puts "## #{ name }:"
    puts "\n" << description if description and not description.empty?
    puts
    puts SOPT.input_doc(inputs, input_types, input_descriptions, input_defaults)

    if deps and deps.any?
      puts
      puts "From dependencies:"
      puts
      deps.each do |dep|
        puts "  #{dep.name}:"
        puts
        puts SOPT.input_doc(dep.inputs, dep.input_types, dep.input_descriptions, dep.input_defaults)
      end
    end
  end
end

module Workflow
  def doc(task = nil)

    if task.nil?
      puts self.to_s 
      puts "=" * self.to_s.length
      puts
      puts "\n" << workflow_description if workflow_description and not workflow_description.empty?
      puts

      puts "## TASKS"
      puts
      tasks.each do |name,task|
        puts "  * #{ name }:"
        puts "    " << task.description if task.description and not task.description.empty?
        puts
      end
    else

      if Task === task
        task_name = task.name
      else
        task_name = task
        task = self.tasks[task_name]
      end
      dependencies = self.rec_dependencies(task_name).collect{|dep_name| self.tasks[dep_name.to_sym]}

      task.doc(dependencies)
    end
  end
end
