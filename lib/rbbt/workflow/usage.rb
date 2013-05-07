
module Task
  def doc(deps = nil)

    puts "## #{ name }:"
    puts "\n" << description if description and not description.empty?
    puts 

    inputs.each do |name|
      short = name.to_s.chars.first

      description = input_descriptions[name]
      default = input_defaults[name]
      type = input_types[name]

      puts "  * -#{short}, --#{name}=<#{ type }>#{default ? " (default: #{default})" : ""}:"
      puts "    " << description if description and not description.empty?
      puts
    end

    if deps and deps.any?
      puts
      puts "From dependencies:"
      puts
      deps.each do |dep|
        puts "  #{dep.name}:"
        puts
        dep.inputs.each do |name|
          short = name.to_s.chars.first

          description = dep.input_descriptions[name]
          default = dep.input_defaults[name]
          type = dep.input_types[name]

          puts "  * -#{short}, --#{name}=<#{ type }>#{default ? " (default: #{default})" : ""}:"
          puts "    " << description if description and not description.empty?
          puts
        end
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
