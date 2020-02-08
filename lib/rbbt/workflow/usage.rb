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
        puts unless Log.compact
        puts SOPT.input_doc(new_inputs, task.input_types, task.input_descriptions, task.input_defaults, true)
        puts unless Log.compact
      end
      puts
    end

    puts Log.color(:magenta, "Returns: ") << Log.color(:blue, result_type.to_s) << "\n"
    puts

    if selects.any?
      puts Log.color(:magenta, "Input select options")
      puts
      selects.collect{|p| p}.uniq.each do |input,options|
        puts Log.color(:blue, input.to_s + ": ") << Misc.format_paragraph(options.collect{|o| o.to_s} * ", ") << "\n"
        puts unless Log.compact
      end
      puts
    end
  end
end

module Workflow

  def dep_tree(name)
    @dep_tree ||= {}
    @dep_tree[name] ||= begin
                        dep_tree = {}
                        self.rec_dependencies(name).each do |dep|
                          dep = dep.first if Array === dep && dep.length == 1

                          workflow, task = case dep
                                           when Array
                                             dep.values_at 0, 1
                                           when Symbol, String
                                             [self, dep]
                                           else
                                             next
                                           end


                          key = [workflow, task]

                          dep_tree[key] = workflow.dep_tree(task)
                        end
                        dep_tree
                      end
  end

  def prov_string(tree)
    description = ""

    last = nil
    seen = Set.new
    tree.collect.to_a.flatten.select{|e| Symbol === e }.each do |task_name|

      child = last && last.include?(task_name)
      first = last.nil?
      last = dep_tree(task_name).collect.to_a.flatten.select{|e| Symbol === e}

      next if seen.include?(task_name)

      if child
        description << "->" << task_name.to_s
      elsif first
        description << "" << task_name.to_s
      else
        description << ";" << task_name.to_s
      end
    end
    description
  end

  def prov_tree(tree, offset = 0, seen = [])

    return "" if tree.empty?

    lines = []

    offset_str = " " * offset

    lines << offset_str 

    tree.each do |p,dtree| 
      next if seen.include?(p)
      seen.push(p)
      workflow, task = p
      lines << offset_str + [workflow.to_s, task.to_s] * "#" + "\n" + workflow.prov_tree(dtree, offset + 1, seen)
    end

    lines * "\n"
  end

  def doc(task = nil, abridge = false)

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

      final = Set.new
      not_final = Set.new
      tasks.each do |name,task|
        tree = dep_tree(name)
        not_final += tree.keys
        final << name unless not_final.include?(name)
      end

      not_final.each do |p|
        final -= [p.last]
      end

      tasks.each do |name,task|
        description = task.description || ""
        description = description.split("\n\n").first

        next if abridge and ! final.include?(name)
        puts Misc.format_definition_list_item(name.to_s, description, Log.terminal_width, 20, :yellow)

        prov_string = prov_string(dep_tree(name))
        puts Log.color :blue, " ->" + prov_string if prov_string && ! prov_string.empty?
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

      prov_tree = prov_tree(dep_tree(task_name))
      if prov_tree && ! prov_tree.empty?

        puts Log.color :magenta, "## DEPENDENCY GRAPH (abridged)"
        puts
        prov_tree.split("\n").each do |line|
          next if line.strip.empty?
          if m = line.match(/^( *)(\w+?)#(\w*)/i)
              offset, workflow, task_name =  m.values_at 1, 2, 3
            puts [offset, Log.color(:magenta, workflow), "#", Log.color(:yellow, task_name)] * ""
          else
            puts Log.color :blue, line 
          end
        end
        puts
      end

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
              puts Misc.format_definition_list_item(input, file.read, Log.terminal_width, 20, :blue)
            end
          end
          puts
        end
      end
    end
  end

  def SOPT_str(task)
    sopt_options = []
    self.rec_inputs(task.name).each do |name|
      short = name.to_s.chars.first
      boolean = self.rec_input_types(task.name)[name].to_sym == :boolean

      sopt_options << "-#{short}--#{name}#{boolean ? "" : "*"}"
    end

    sopt_options * ":"
  end

  def get_SOPT(task)
    sopt_option_string = self.SOPT_str(task)
    SOPT.get sopt_option_string
  end

  def self.get_SOPT(workflow, task)
    workflow = Workflow.require_workflow workflow if String === workflow
    task = workflow.tasks[task.to_sym] if String === task || Symbol === task
    workflow.get_SOPT(task)
  end
end
