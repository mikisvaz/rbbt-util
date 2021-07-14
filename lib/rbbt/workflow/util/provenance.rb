class Step

  def self.status_color(status)
    case status.to_sym
    when :error, :aborted, :missing, :dead, :unsync
      :red
    when :streaming, :started
      :cyan
    when :done, :noinfo
      :green
    when :dependencies, :waiting, :setup
      :yellow
    when :notfound, :cleaned
      :blue
    else
      if status.to_s.index ">"
        :cyan
      else
        :cyan
      end
    end
  end

  def self.prov_status_msg(status)
    color = status_color(status)
    Log.color(color, status.to_s)
  end

  def self.prov_report_msg(status, name, path, info, input = nil)
    parts = path.sub(/\{.*/,'').split "/"

    parts.pop

    task = Log.color(:yellow, parts.pop)
    workflow = Log.color(:magenta, parts.pop)
    #if status.to_s == 'noinfo' && parts.last != 'jobs'
    if ! Workflow.job_path?(path)
      task, status, workflow = Log.color(:yellow, info[:task_name]), Log.color(:green, "file"), Log.color(:magenta, "-")
    end

    path_mtime = begin
                   Open.mtime(path)
                 rescue Exception
                   nil
                 end

    if input.nil? || input.empty?
      input_str = nil
    else
      input = input.reject{|dep,name| (input & dep.dependencies.collect{|d| [d,name]}).any? }
      input = input.reject{|dep,name| (input & dep.input_dependencies.collect{|d| [d,name]}).any? }
      input_str = Log.color(:magenta, "-> ") + input.collect{|dep,name| Log.color(:yellow, dep.task_name.to_s) + ":" + Log.color(:yellow, name) }.uniq * " "
    end

    str = if ! (Open.remote?(path) || Open.ssh?(path)) && (Open.exists?(path) && $main_mtime && path_mtime && ($main_mtime - path_mtime) < -2)
            prov_status_msg(status.to_s) << " " << [workflow, task, path, input_str].compact * " " << " (#{Log.color(:red, "Mtime out of sync") })"
          else
            prov_status_msg(status.to_s) << " " << [workflow, task, path, input_str].compact * " " 
          end

    if $inputs and $inputs.any? 
      job_inputs = Workflow.load_step(path).recursive_inputs.to_hash
      IndiferentHash.setup(job_inputs)

      $inputs.each do |input|
        value = job_inputs[input]
        next if  value.nil?
        value_str = Misc.fingerprint(value)
        str << "\t#{Log.color :magenta, input}=#{value_str}"
      end
    end

    if $info_fields and $info_fields.any?
      $info_fields.each do |field|
        IndiferentHash.setup(info)
        value = info[field]
        next if value.nil?
        value_str = Misc.fingerprint(value)
        str << "\t#{Log.color :magenta, field}=#{value_str}"
      end
    end

    str << "\n"
  end

  def self.prov_report(step, offset = 0, task = nil, seen = [], expand_repeats = false, input = nil)
    info = step.info  || {}
    info[:task_name] = task
    path  = step.path
    status = info[:status] || :missing
    status = "remote" if Open.remote?(path) || Open.ssh?(path)
    name = info[:name] || File.basename(path)
    status = :unsync if status == :done and not Open.exist?(path)
    status = :notfound if status == :noinfo and not Open.exist?(path)


    this_step_msg = prov_report_msg(status, name, path, info, input)

    input_dependencies = {}
    step.dependencies.each do |dep|
      if dep.input_dependencies.any?
        dep.input_dependencies.each do |id|
          input_name = dep.recursive_inputs.fields.zip(dep.recursive_inputs).select{|f,d| 
            d == id || (String === d && d.start_with?(id.files_dir)) || (Array === d && d.include?(id))
          }.last.first
          input_dependencies[id] ||= []
          input_dependencies[id] << [dep, input_name]
        end
      end
    end

    str = ""
    str = " " * offset + this_step_msg if ENV["RBBT_ORIGINAL_STACK"] == 'true'

    step.dependencies.dup.tap{|l| 
      l.reverse! if ENV["RBBT_ORIGINAL_STACK"] == 'true'
    }.each do |dep|
      path = dep.path
      new = ! seen.include?(path)
      if new
        seen << path
        str << prov_report(dep, offset + 1, task, seen, expand_repeats, input_dependencies[dep])
      else
        if expand_repeats
          str << Log.color(Step.status_color(dep.status), Log.uncolor(prov_report(dep, offset+1, task)))
        else
          info = dep.info  || {}
          status = info[:status] || :missing
          status = "remote" if Open.remote?(path) || Open.ssh?(path)
          name = info[:name] || File.basename(path)
          status = :unsync if status == :done and not Open.exist?(path)
          status = :notfound if status == :noinfo and not Open.exist?(path)

          str << Log.color(Step.status_color(status), " " * (offset + 1) + Log.uncolor(prov_report_msg(status, name, path, info, input_dependencies[dep])))
        end
      end
    end if step.dependencies

    str += (" " * offset) + this_step_msg unless ENV["RBBT_ORIGINAL_STACK"] == 'true'

    str
  end
end
