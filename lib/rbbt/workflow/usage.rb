require 'rbbt/util/simpleopt'

module Task
  def doc(deps = nil)
    puts Log.color :magenta, "## #{ name }:"
    puts "\n" << description  << "\n" if description and not description.empty?
    puts

    case
    when (input_types.values & [:array]).any?
      puts Log.color(:green, Misc.format_paragraph("Lists are specified as arguments using ',' or '|'. When specified as files the '\\n'
      also works in addition to the others. You may use the '--array_separator' option
      the change this default. Whenever a file is specified it may also accept STDIN using
      the '-' character."))
      puts

    when (input_types.values & [:text, :tsv]).any?
      puts Log.color(:green, Misc.format_paragraph("Whenever a file is specified it may also accept STDIN using the '-' character."))
      puts
    end

    puts SOPT.input_doc(inputs, input_types, input_descriptions, input_defaults, true)
    puts

    puts "Returns: " << Log.color(:blue, result_type.to_s) << "\n"
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
      if self.documentation[:description] and not self.documentation[:description].empty?
        puts
        puts Misc.format_paragraph self.documentation[:description] 
      end
      puts

      puts Log.color :magenta, "## TASKS"
      if self.documentation[:task_description] and not self.documentation[:task_description].empty?
        puts
        puts Misc.format_paragraph self.documentation[:task_description] 
      end
      puts

      tasks.each do |name,task|
        puts Misc.format_definition_list_item(name.to_s, task.description || "", 80, 30, :yellow)
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
