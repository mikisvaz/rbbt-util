class Step
  def self.link_job(path, target_dir, task = nil, workflow = nil)
    Path.setup(target_dir)

    name = File.basename(path)
    task = File.basename(File.dirname(path)) if task.nil?
    workflow = File.basename(File.dirname(File.dirname(path))) if workflow.nil?

    return if target_dir[workflow][task][name].exists? || File.symlink?(target_dir[workflow][task][name].find)
    Log.debug "Linking #{ path }"
    FileUtils.mkdir_p target_dir[workflow][task] unless target_dir[workflow][task].exists?
    FileUtils.ln_s path, target_dir[workflow][task][name].find if File.exists?(path)
    FileUtils.ln_s path + '.files', target_dir[workflow][task][name].find + '.files' if File.exists?(path + '.files')
    FileUtils.ln_s path + '.info', target_dir[workflow][task][name].find + '.info' if File.exists?(path + '.info')
  end

  def archive(target = nil)
    target = self.path + '.tar.gz' if target.nil?
    target = File.expand_path(target)
    TmpFile.with_file do |tmpdir|
      Step.link_job self.path, tmpdir
      rec_dependencies = Set.new
      deps = [self.path]
      seen = Set.new
      while deps.any?
        path = deps.shift
        dep = Step.new path
        seen << dep.path
        dep.info[:dependencies].each do |task, name, path|
          dep = Step.new path
          next if seen.include? dep.path
          deps << dep.path
          rec_dependencies << dep.path
        end if dep.info[:dependencies]
      end

      rec_dependencies.each do |path|
        Step.link_job path, tmpdir
      end

      Misc.in_dir(tmpdir) do
        if File.directory?(target)
          CMD.cmd_log("rsync -avzHP --copy-unsafe-links '#{ tmpdir }/' '#{ target }/'")
        else
          CMD.cmd_log("tar cvhzf '#{target}'  ./*")
        end
      end
      Log.debug "Archive finished at: #{target}"
    end
  end

  def self.job_files_for_archive(files)
    job_files = Set.new

    jobs = files.collect do |file|  
      if Step === file
        file
      else
        file = file.sub(/\.info$/,'')
        Step.new(File.expand_path(file))
      end
    end.uniq

    jobs.each do |step|
      next unless File.exists?(step.path)
      job_files << step.path
      job_files << step.info_file if File.exists?(step.info_file)
      job_files << step.files_dir if Dir.glob(step.files_dir + '/*').any?
      rec_dependencies = Set.new
      deps = [step.path]
      seen = Set.new
      while deps.any?
        path = deps.shift
        dep = Step.new path
        seen << dep.path
        dep.info[:dependencies].each do |task, name, path|
          dep = Step.new path
          next if seen.include? dep.path
          deps << dep.path
          rec_dependencies << dep.path
        end if dep.info[:dependencies]
      end

      rec_dependencies.each do |path|
        next unless File.exists?(path)
        job_files << dep.files_dir if Dir.glob(dep.files_dir + '/*').any?
        job_files << dep.info_file if File.exists?(dep.info_file)
        job_files << path
      end
    end

    job_files.to_a
  end

  def self.archive(files, target = nil)
    target = self.path + '.tar.gz' if target.nil?
    target = File.expand_path(target) if String === target

    job_files = job_files_for_archive files
    TmpFile.with_file do |tmpdir|
      job_files.each do |file|
        Step.link_job file, tmpdir
      end

      Misc.in_dir(tmpdir) do
        if File.directory?(target)
          CMD.cmd_log("rsync -avzHP --copy-unsafe-links '#{ tmpdir }/' '#{ target }/'")
        else
          CMD.cmd_log("tar cvhzf '#{target}'  ./*")
        end
      end
      Log.debug "Archive finished at: #{target}"
    end
  end
end