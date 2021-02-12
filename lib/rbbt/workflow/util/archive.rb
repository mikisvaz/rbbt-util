class Step

  MAIN_RSYNC_ARGS="-avztAXHP"
  
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
        dep = Workflow.load_step path
        seen << dep.path
        dep.dependencies.each do |dep|
          next if seen.include? dep.path
          deps << dep.path
          rec_dependencies << dep.path
        end if dep.dependencies
      end

      rec_dependencies.each do |path|
        Step.link_job path, tmpdir
      end

      Misc.in_dir(tmpdir) do
        if File.directory?(target)
          CMD.cmd_log("rsync #{MAIN_RSYNC_ARGS} --copy-unsafe-links '#{ tmpdir }/' '#{ target }/'")
        else
          CMD.cmd_log("tar cvhzf '#{target}'  ./*")
        end
      end
      Log.debug "Archive finished at: #{target}"
    end
  end

  def self.job_files_for_archive(files, recursive = false, skip_overriden = false)
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
      next if skip_overriden && step.overriden

      job_files << step.path
      job_files << step.info_file if File.exists?(step.info_file)
      job_files << Step.md5_file(step.path) if File.exists?(Step.md5_file step.path)
      job_file_dir_content = Dir.glob(step.files_dir + '/**/*')
      job_files += job_file_dir_content
      job_files << step.files_dir if File.exists?(step.files_dir)
      rec_dependencies = Set.new

      next unless recursive

      deps = [step.path]
      seen = Set.new
      while deps.any?
        path = deps.shift

        dep = Workflow.load_step path
        seen << dep.path

        dep.load_dependencies_from_info

        dep.dependencies.each do |dep|
          next if seen.include? dep.path
          deps << dep.path
          rec_dependencies << dep.path
        end if dep.info[:dependencies]
      end

      rec_dependencies.each do |path|
        dep = Workflow.load_step path
        job_files << dep.path
        job_files << dep.files_dir if Dir.glob(dep.files_dir + '/*').any?
        job_files << dep.info_file if File.exists?(dep.info_file)
      end
    end

    job_files.to_a
  end

  def self.archive(files, target = nil, recursive = true)
    target = self.path + '.tar.gz' if target.nil?
    target = File.expand_path(target) if String === target

    job_files = job_files_for_archive files, recursive
    TmpFile.with_file do |tmpdir|
      job_files.each do |file|
        Step.link_job file, tmpdir
      end

      Misc.in_dir(tmpdir) do
        if File.directory?(target)
          CMD.cmd_log("rsync #{MAIN_RSYNC_ARGS} --copy-unsafe-links '#{ tmpdir }/' '#{ target }/'")
        else
          CMD.cmd_log("tar cvhzf '#{target}'  ./*")
        end
      end
      Log.debug "Archive finished at: #{target}"
    end
  end

  def self.migrate(path, search_path, options = {})
    resource=Rbbt

    orig_path = path
    other_rsync_args = options[:rsync]

    recursive = options[:recursive]
    recursive = false if recursive.nil?

    paths = if options[:source]
              Misc.ssh_run(options[:source], <<-EOF).split("\n")
require 'rbbt-util'
require 'rbbt/workflow'

path = "#{path}"
recursive = #{ recursive.to_s }

if File.exists?(path)
  path = #{resource.to_s}.identify(path)
else
  path = Path.setup(path)
end

files = path.glob_all

files = Step.job_files_for_archive(files, recursive)

puts files * "\n"
              EOF

            else
              if File.exists?(path)
                path = resource.identify(path)
                raise "Resource #{resource} could not identify #{orig_path}" if path.nil?
              else
                path = Path.setup(path)
              end
              files = path.glob_all
              files = Step.job_files_for_archive(files, recursive)
              files
            end


    target = if options[:target] 
               target = Misc.ssh_run(options[:target], <<-EOF).split("\n").first
require 'rbbt-util'
path = "var/jobs"
resource = #{resource.to_s}
search_path = "#{search_path}"
puts resource[path].find(search_path)
               EOF
             else
               resource['var/jobs'].find(search_path)
             end

    subpath_files = {}
    paths.sort.each do |path|
      parts = path.split("/")
      subpath = parts[0..-4] * "/" + "/"

      if subpath_files.keys.any? && subpath.start_with?(subpath_files.keys.last)
        subpath = subpath_files.keys.last
      end

      source = path[subpath.length..-1]

      subpath_files[subpath] ||= []
      subpath_files[subpath] << source
    end

    synced_files = []
    subpath_files.each do |subpath, files|
      if options[:target]
        CMD.cmd("ssh #{options[:target]} mkdir -p '#{File.dirname(target)}'")
      else
        Open.mkdir File.dirname(target)
      end

      if options[:source]
        source = [options[:source], subpath] * ":"
      else
        source = subpath
      end
      target = [options[:target], target] * ":" if options[:target]

      next if File.exists?(source) && File.exists?(target) && File.expand_path(source) == File.expand_path(target)

      files_and_dirs = Set.new( files )
      files.each do |file|
        synced_files << File.join(subpath, file)

        parts = file.split("/")[0..-2].reject{|p| p.empty?}
        while parts.any?
          files_and_dirs <<  parts * "/"
          parts.pop
        end
      end

      TmpFile.with_file(files_and_dirs.sort_by{|l| l.length}.to_a * "\n") do |tmp_include_file|
        test_str = options[:test] ? '-nv' : ''

        cmd = "rsync #{MAIN_RSYNC_ARGS} --progress #{test_str} --files-from='#{tmp_include_file}' #{source}/ #{target}/ #{other_rsync_args}"

        #cmd << " && rm -Rf #{source}" if options[:delete]
        if options[:print]
          ppp Open.read(tmp_include_file)
          puts cmd 
        else
          CMD.cmd_log(cmd, :log => Log::INFO)
        end
      end
    end

    if options[:delete] && synced_files.any?
      puts Log.color :magenta, "About to erase these files:"
      synced_files.each do |p|
        puts Log.color :red, p
      end 

      if options[:non_interactive]
        response = 'yes'
      else
        puts Log.color :magenta, "Type 'yes' if you are sure:"
        response = STDIN.gets.chomp
      end

      if response == 'yes'
        synced_files.each do |p|
          Open.rm p
        end
      end
    end
  end

  def self.purge(path, recursive = false, skip_overriden = true)
    path = [path] if String === path
    job_files = job_files_for_archive path, recursive, skip_overriden

    job_files.each do |file|
      begin
        Log.debug "Purging #{file}"
        Open.rm_rf file if Open.exists?(file)
      rescue
        Log.warn "Could not erase '#{file}': #{$!.message}"
      end
    end
  end
end
