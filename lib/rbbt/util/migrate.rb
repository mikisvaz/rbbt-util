module Rbbt 

  def self.migrate_source_paths(path, resource = Rbbt, source = nil)
    if source
      lpath, *paths = SSHLine.ruby(source, <<-EOF).split("\n")
require 'rbbt-util'
path = "#{path}"
if Open.exists?(path)
  path = Path.setup(#{resource.to_s}.identify(path))
else
  path = Path.setup(path)
end
puts path
puts path.find_all.collect{|p| File.directory?(p) ? p + "/" : p } * "\n"
      EOF

      [path, paths.collect{|p| [source, p] * ":"}, lpath]
    else
      original_path = Path.setup(path)
      if File.exist?(path)
        path = resource.identify(path)
      else
        path = Path.setup(path)
      end

      if original_path.located?
        paths = [original_path]
      else
        path = Path.setup(path) unless Path === path
        paths = (path.directory? ? path.glob_all : path.find_all)
      end

      [path, paths, path]
    end
  end

  def self.migrate_target_path(path, search_path = 'user', resource = Rbbt, target = nil)
    if target
      SSHLine.ruby(target, <<-EOF).split("\n").first
require 'rbbt-util'
path = "#{path}"
resource = #{resource.to_s}
search_path = "#{search_path}"
puts resource[path].find(search_path)
      EOF
    else
      resource[path].find(search_path)
    end
  end

  def self.migrate_files(real_paths, target, options = {})
    excludes = %w(.save .crap .source tmp filecache open-remote)
    excludes += (options[:exclude] || "").split(/,\s*/)
    excludes_str = excludes.collect{|s| "--exclude '#{s}'" } * " "

    hard_link = options[:hard_link]

    other = options[:other] || []

    test_str = options[:test] ? '-nv' : ''

    real_paths.each do |source_path|
      Log.low "Migrating #{source_path} #{options[:files].length} files to #{target} - #{Misc.fingerprint(options[:files])}}" if options[:files]
      if File.directory?(source_path) || source_path.end_with?("/")
        source_path += "/" unless source_path.end_with? '/'
        target += "/" unless target.end_with? '/'
      end

      next if source_path == target && ! (options[:source] || options[:target])

      if options[:target]
        CMD.cmd("ssh #{options[:target]} mkdir -p '#{File.dirname(target)}'")
      else
        Open.mkdir File.dirname(target)
      end

      if options[:target]
        target_path = [options[:target], "'" + target + "'"] * ":" 
      else
        target_path = "'" + target + "'"
      end

      TmpFile.with_file do |tmp_files|
        if options[:files]
          Open.write(tmp_files, options[:files] * "\n")
          files_from_str = "--files-from='#{tmp_files}'"
        else
          files_from_str = ""
        end

        #cmd = "rsync -avztAXHP --copy-unsafe-links #{test_str} #{files_from_str} #{excludes_str} '#{source_path}' #{target_path} #{other * " "}"
         
        # rsync_args = "-avztAXHP --copy-unsafe-links"
        rsync_args = "-avztHP --copy-unsafe-links --omit-dir-times"

        rsync_args << " --link-dest '#{source_path}'" if hard_link && ! options[:source]

        cmd = "rsync #{rsync_args} #{test_str} #{files_from_str} #{excludes_str} '#{source_path}' #{target_path} #{other * " "}"

        cmd << " && rm -Rf #{source_path}" if options[:delete] && ! options[:files]

        if options[:print]
          puts cmd 
          exit 0
        else
          CMD.cmd_log(cmd, :log => Log::HIGH)

          if options[:delete] && options[:files]
            remove_files = options[:files].collect{|f| File.join(source_path, f) }
            dirs = remove_files.select{|f| File.directory? f }
            remove_files.each do |file|
              next if dirs.include? file
              Open.rm file
            end
            dirs.each do |dir|
              FileUtils.rmdir dir if Dir.glob(dir).empty?
            end
          end 
        end
      end
    end
  end


  def self.migrate(path, search_path, options = {})
    search_path = 'user' if search_path.nil?

    resource = Rbbt

    path, real_paths, lpath = migrate_source_paths(path, resource, options[:source])

    target = migrate_target_path(lpath, search_path, resource, options[:target])

    migrate_files(real_paths, target, options)
  end
end
