require 'rbbt/util/migrate'
class Step

  MAIN_RSYNC_ARGS="-avztAXHP --copy-links"
  
  def self.link_job(path, target_dir, task = nil, workflow = nil)
    Path.setup(target_dir)

    name = File.basename(path)
    task = File.basename(File.dirname(path)) if task.nil?
    workflow = File.basename(File.dirname(File.dirname(path))) if workflow.nil?

    return if target_dir[workflow][task][name].exists? || File.symlink?(target_dir[workflow][task][name].find)
    Log.debug "Linking #{ path }"
    FileUtils.mkdir_p target_dir[workflow][task] unless target_dir[workflow][task].exists?
    FileUtils.ln_s path, target_dir[workflow][task][name].find if File.exist?(path)
    FileUtils.ln_s path + '.files', target_dir[workflow][task][name].find + '.files' if File.exist?(path + '.files')
    FileUtils.ln_s path + '.info', target_dir[workflow][task][name].find + '.info' if File.exist?(path + '.info')
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
      next unless File.exist?(step.path)
      next if skip_overriden && step.overriden

      job_files << step.path
      job_files << step.info_file if File.exist?(step.info_file)
      job_files << Step.md5_file(step.path) if File.exist?(Step.md5_file step.path)
      job_file_dir_content = Dir.glob(step.files_dir + '/**/*')
      job_files += job_file_dir_content
      job_files << step.files_dir if File.exist?(step.files_dir)
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
        job_files << dep.info_file if File.exist?(dep.info_file)
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

  def self.migrate_source_paths(path, resource = Rbbt, source = nil, recursive = true)
    recursive = false if recursive.nil?
    if source
      lpath, *paths = Misc.ssh_run(source, <<-EOF).split("\n")
require 'rbbt-util'
require 'rbbt/workflow'

recursive = #{ recursive.to_s }
path = "#{path}"

if Open.exists?(path)
  path = #{resource.to_s}.identify(path)
else
  path = Path.setup(path)
end

files = path.glob_all.collect{|p| File.directory?(p) ? p + "/" : p }
files = Step.job_files_for_archive(files, recursive)

puts path
puts files * "\n"
      EOF

      [path, paths.collect{|p| [source, p] * ":"}, lpath]
    else
      path = Path.setup(path.dup)
      files = path.glob_all
      files = Step.job_files_for_archive(files, recursive)

      [path, files, path]
    end
  end

  def self.migrate(path, search_path, options = {})
    if Step === path
      if options[:source]
        path = Rbbt.identify(path.path)
      else
        path = path.path
      end
    end
    search_path = 'user' if search_path.nil?

    resource = Rbbt

    path, real_paths, lpath = self.migrate_source_paths(path, resource, options[:source], options[:recursive])

    subpath_files = {}
    real_paths.sort.each do |path|
      parts = path.split("/")
      subpath = parts[0..-4] * "/" + "/"

      if subpath_files.keys.any? && subpath.start_with?(subpath_files.keys.last)
        subpath = subpath_files.keys.last
      end

      source = path.chars[subpath.length..-1] * ""

      subpath_files[subpath] ||= []
      subpath_files[subpath] << source
    end

    target = Rbbt.migrate_target_path('var/jobs', search_path, resource, options[:target])

    target_path = File.join(target, *path.split("/")[-3..-1])

    subpath_files.each do |subpath, files|
      Rbbt.migrate_files([subpath], target, options.merge(:files => files))
    end

    target_path
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
