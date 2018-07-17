require 'rbbt/util/simpleopt'

module Task
  def doc(workflow = nil, deps = nil)
    puts Log.color(:yellow, "## #{ name }") << ":"
    puts "\n" << Misc.format_paragraph(description.strip)  << "\n" if description and not description.empty?
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

    selects = []
    if inputs.any?
      inputs.zip(input_types.values_at(*inputs)).select{|i,t| t.to_sym == :select and input_options[i][:select_options] }.each{|i,t| selects << [i, input_options[i][:select_options]]  }
      puts SOPT.input_doc(inputs, input_types, input_descriptions, input_defaults, true)
      puts
    end

    if deps and deps.any?
      puts Log.color(:magenta, "Inputs from dependencies:")
      puts
      seen = []
      task_inputs = dep_inputs deps, workflow
      task_inputs.each do |task,new_inputs|
        new_inputs.zip(task.input_types.values_at(*new_inputs)).select do |i,t| 
          t.to_sym == :select and task.input_options[i][:select_options] 
        end.each do |i,t| 
          selects << [i, task.input_options[i][:select_options]] 
        end

        if task.workflow and task.workflow != workflow
          puts "  #{Log.color :yellow, ["[#{task.workflow.to_s}]", task.name.to_s] *" "}:"
        else
          puts "  #{Log.color :yellow, task.name.to_s}:"
        end
        puts
        puts SOPT.input_doc(new_inputs, task.input_types, task.input_descriptions, task.input_defaults, true)
        puts
      end
    end

    puts Log.color(:magenta, "Returns: ") << Log.color(:blue, result_type.to_s) << "\n"
    puts
    
    if selects.any?
      puts Log.color(:magenta, "Input select options")
      puts
      selects.collect{|p| p}.uniq.each do |input,options|
        puts Log.color(:blue, input.to_s + ": ") << Misc.format_paragraph(options.collect{|o| o.to_s} * ", ") << "\n"
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

      if self.documentation[:title] and not self.documentation[:title].empty?
        puts
        puts Misc.format_paragraph self.documentation[:title] 
      end

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
        description = task.description || ""
        description = description.split("\n\n").first
        puts Misc.format_definition_list_item(name.to_s, description, 80, 30, :yellow)
      end

    else

      if Task === task
        task_name = task.name
      else
        task_name = task
        task = self.tasks[task_name]
      end

      #dependencies = self.rec_dependencies(task_name).collect{|dep_name| Array === dep_name ? dep_name.first.tasks[dep_name[1].to_sym] : self.tasks[dep_name.to_sym]}
      task.doc(self, self.rec_dependencies(task_name))

      if self.examples.include? task_name
          self.examples[task_name].each do |example|

            puts Log.color(:magenta, "Example ") << Log.color(:green, example) + " -- " + Log.color(:blue, example_dir[task_name][example])

            inputs = self.example(task_name, example)

            inputs.each do |input, type, file|
                case type
                when :tsv, :array, :text
                  lines = file.read.split("\n")
                  head = lines[0..5].compact * "\n\n"
                  head = head[0..500]
                  puts Misc.format_definition_list_item(input, head, 1000, -1, :blue).gsub(/\n\s*\n/,"\n") 
                  puts '...' if lines.length > 6
                else
                  puts Misc.format_definition_list_item(input, file.read, 80, 20, :blue)
                end
            end
            puts
          end
        end
      end
  end
end
