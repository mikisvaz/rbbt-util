module Resource
  def set_software_env(software_dir = self.root.software)
    software_dir.opt.find_all.collect{|d| d.annotate(File.dirname(d)) }.reverse.each do |software_dir|
      next unless software_dir.exists?
      Log.medium "Preparing software env at #{software_dir}"

      software_dir = File.expand_path(software_dir)
      opt_dir = File.join(software_dir, 'opt')
      bin_dir = File.join(opt_dir, 'bin')

      Misc.env_add 'PATH', bin_dir

      FileUtils.mkdir_p opt_dir unless File.exist? opt_dir

      %w(.ld-paths .c-paths .pkgconfig-paths .aclocal-paths .java-classpaths).each do |file|
        filename = File.join(opt_dir, file)
        begin
          FileUtils.touch filename unless File.exist? filename
        rescue
          Log.warn("Could not touch #{ filename }")
        end
      end

      Open.read(File.join opt_dir, '.c-paths').split(/\n/).each do |line|
        dir = line.chomp
        dir = File.join(opt_dir, dir) unless dir[0] == "/"
        Misc.env_add('CPLUS_INCLUDE_PATH',dir)
        Misc.env_add('C_INCLUDE_PATH',dir)
      end if File.exist? File.join(opt_dir, '.c-paths')

      Open.read(File.join opt_dir, '.ld-paths').split(/\n/).each do |line|
        dir = line.chomp
        dir = File.join(opt_dir, dir) unless dir[0] == "/"
        Misc.env_add('LIBRARY_PATH',dir)
        Misc.env_add('LD_LIBRARY_PATH',dir)
        Misc.env_add('LD_RUN_PATH',dir)
      end if File.exist? File.join(opt_dir, '.ld-paths')

      Open.read(File.join opt_dir, '.pkgconfig-paths').split(/\n/).each do |line|
        dir = line.chomp
        dir = File.join(opt_dir, dir) unless dir[0] == "/"
        Misc.env_add('PKG_CONFIG_PATH',dir)
      end if File.exist? File.join(opt_dir, '.pkgconfig-paths')

      Open.read(File.join opt_dir, '.aclocal-paths').split(/\n/).each do |line|
        dir = line.chomp
        dir = File.join(opt_dir, dir) unless dir[0] == "/"
        Misc.env_add('ACLOCAL_FLAGS', "-I #{dir}", ' ')
      end if File.exist? File.join(opt_dir, '.aclocal-paths')

      Open.read(File.join opt_dir, '.java-classpaths').split(/\n/).each do |line|
        dir = line.chomp
        dir = File.join(opt_dir, dir) unless dir[0] == "/"
        Misc.env_add('CLASSPATH', "#{dir}")
      end if File.exist? File.join(opt_dir, '.java-classpaths')

      Dir.glob(File.join opt_dir, 'jars', '*.jar').each do |file|
        Misc.env_add('CLASSPATH', "#{file}")
      end

      if File.exist?(File.join(opt_dir, '.post_install')) and File.directory?(File.join(opt_dir, '.post_install'))
        Dir.glob(File.join(opt_dir, '.post_install','*')).each do |file|

          # Load exports
          Open.read(file).split("\n").each do |line|
            next unless line =~ /^\s*export\s+([^=]+)=(.*)/
            var = $1.strip
            value = $2.strip
            value.sub!(/^['"]/,'')
            value.sub!(/['"]$/,'')
            value.gsub!(/\$[a-z_0-9]+/i){|var| ENV[var[1..-1]] }
            Log.debug "Set variable export from .post_install: #{Misc.fingerprint [var,value]*"="}"
            ENV[var] = value
          end
        end
      end
    end
  end


  def rake_for(path)
    @rake_dirs.select{|dir, content|
      Misc.common_path(dir, path)
    }.sort_by{|dir, content|
      dir.length
    }.last
  end

  def has_rake(path)
    !! rake_for(path)
  end

  def run_rake(path, rakefile, rake_dir)
    task = Misc.path_relative_to rake_dir, path
    rakefile = rakefile.produce if rakefile.respond_to? :produce
    rakefile = rakefile.find if rakefile.respond_to? :find

    rake_dir = rake_dir.find(:user) if rake_dir.respond_to? :find

    begin
      require 'rbbt/resource/rake'
      if Proc === rakefile
        Rake.run(nil, rake_dir, task, &rakefile)
      else
        Rake.run(rakefile, rake_dir, task)
      end
    rescue Rake::TaskNotFound
      raise $! if rake_dir.nil? or rake_dir.empty? or rake_dir == "/" or rake_dir == "./"
      task = File.join(File.basename(rake_dir), task)
      rake_dir = File.dirname(rake_dir)
      retry
    end
  end
end
