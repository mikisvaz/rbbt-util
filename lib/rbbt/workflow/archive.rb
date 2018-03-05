class Step
  def self.link_job(path, target_dir, task = nil, workflow = nil)
    Path.setup(target_dir)

    name = File.basename(path)
    task = File.basename(File.dirname(path)) if task.nil?
    workflow = File.basename(File.dirname(File.dirname(path))) if workflow.nil?

    FileUtils.mkdir_p target_dir[workflow][task]
    FileUtils.ln_s path, target_dir[workflow][task][name].find
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
        io = CMD.cmd("tar cvhzf '#{target}'  ./*", :pipe => true)
        while line = io.gets
          Log.debug line
        end
        io.join if io.respond_to? :join
      end
    end
  end
end
