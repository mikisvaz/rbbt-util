module Docker
  def self.run(image, cmd, options = {})
    mounts, job_inputs, directory, pipe = Misc.process_options options, :mounts, :job_inputs, :directory, :pipe

    if mounts
      mounts.each{|t,s| FileUtils.mkdir_p s unless File.exist? s}
      mount_cmd = mounts.sort.collect{|t,s| "-v " + ["'" + s + "'", "'" + t + "'"] * ":" } * " "
    else
      mount_cmd = ""
    end

    image_cmd = "-t #{image}"

    if directory
      Path.setup(directory) unless Path === directory
      FileUtils.mkdir_p directory unless File.directory? directory
      mount_cmd += " -v '#{directory}':/job"
      job_inputs.each do |name,obj|
        case obj
        when File 
          Open.ln_h Open.realpath(obj.filename), directory[name]
        when IO
          begin
            Open.write(directory[name], obj)
          ensure
            obj.join if obj.respond_to?(:join) and not obj.joined?
          end
        when String
          if obj.length < 256 and File.exist?(obj)
            Open.ln_h Open.realpath(obj), directory[name]
          else
            Open.write(directory[name], obj)
          end
        end
      end if job_inputs
      cmd = "docker run #{mount_cmd} #{image_cmd} #{cmd}"
      if pipe
        CMD.cmd(cmd, :log => true, :pipe => true)
      else
        CMD.cmd_log(cmd, :log => true)
      end
    else
      TmpFile.with_file do |tmpfile|
        Path.setup(tmpfile)
        Open.mkdir tmpfile
        mount_cmd += " -v '#{tmpfile}':/job"
        job_inputs.each do |name,obj|
          case obj
          when File 
            Open.ln_h Open.realpath(obj.filename), tmpfile[name]
          when IO
            begin
              Open.write(tmpfile[name], obj)
            ensure
              obj.join if obj.respond_to?(:join) and not obj.joined?
            end
          when String
            if obj.length < 256 and File.exist?(obj)
              Open.ln_h Open.realpath(obj), tmpfile[name]
            else
              Open.write(tmpfile[name], obj)
            end
          end
        end if job_inputs
        cmd = "docker run #{mount_cmd} #{image_cmd} #{cmd}"
        CMD.cmd_log(cmd, :log => true)
      end
    end
  end
end
