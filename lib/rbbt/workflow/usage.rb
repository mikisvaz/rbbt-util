require 'rbbt/util/simpleopt'

module Task
  def doc(deps = nil)
    puts Log.color :magenta, "## #{ name }:"
    puts "\n" << description  << "\n" if description and not description.empty?
    puts "Returns: " << Log.color(:blue, result_type.to_s) << "\n"
    puts SOPT.input_doc(inputs, input_types, input_descriptions, input_defaults, true)
    puts


    if deps and deps.any?
      puts "From dependencies:"
      puts
      deps.each do |dep|
        puts "  #{dep.name}:"
        puts
        puts SOPT.input_doc((dep.inputs - self.inputs), dep.input_types, dep.input_descriptions, dep.input_defaults, true)
        puts
      end
    end
  end
end

module Workflow
  def doc(task = nil)

    if task.nil?
      puts Log.color :magenta, self.to_s 
      puts Log.color :magenta, "=" * self.to_s.length
      puts
      puts "\n" << workflow_description if workflow_description and not workflow_description.empty?
      puts

      puts Log.color :magenta, "## TASKS"
      puts
      tasks.each do |name,task|
        puts "  * #{ Log.color :green, name.to_s }:"
        puts "    " << task.description.split(/\n\s*\n/).first if task.description and not task.description.empty?
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
