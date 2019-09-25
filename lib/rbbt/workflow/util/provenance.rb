class Step
  def self.prov_status_msg(status)
    color = case status.to_sym
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
    Log.color(color, status.to_s)
  end

  def self.prov_report_msg(status, name, path, info = nil)
    parts = path.sub(/\{.*/,'').sub(/#{Regexp.quote(name)}$/,'').split "/"

    task = Log.color(:yellow, parts.pop)
    workflow = Log.color(:magenta, parts.pop)
    if status.to_s == 'noinfo' and parts.last != 'jobs'
      task, status, workflow = Log.color(:yellow, info[:task_name]), Log.color(:green, "file"), Log.color(:magenta, "-")
    end

    path_mtime = begin
                   Open.mtime(path)
                 rescue
                   nil
                 end
    str = if not Open.remote?(path) and (Open.exists?(path) and $main_mtime and path_mtime and ($main_mtime - path_mtime) < -2)
            prov_status_msg(status.to_s) << " " << [workflow, task, path].compact * " " << " (#{Log.color(:red, "Mtime out of sync") })"
          else
            prov_status_msg(status.to_s) << " " << [workflow, task, path].compact * " " 
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

  def self.prov_report(step, offset = 0, task = nil, seen = [])
    info = step.info  || {}
    info[:task_name] = task
    path  = step.path
    status = info[:status] || :missing
    status = "remote" if Open.remote?(path)
    name = info[:name] || File.basename(path)
    status = :unsync if status == :done and not Open.exist?(path)
    status = :notfound if status == :noinfo and not Open.exist?(path)
    str = " " * offset
    str << prov_report_msg(status, name, path, info)
    step.dependencies.reverse.each do |dep|
      path = dep.path
      new = ! seen.include?(path)
      if new
        seen << path
        str << prov_report(dep, offset + 1, task, seen)
      else
        str << Log.color(:green, Log.uncolor(prov_report(dep, offset+1, task)))
      end
    end if step.dependencies
    str
  end
end
