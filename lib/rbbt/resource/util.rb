module Path

  def self.caller_lib_dir(file = nil, relative_to = ['lib', 'bin'])
    file = caller.reject{|l| 
      l =~ /rbbt\/(?:resource\.rb|workflow\.rb)/ or
      l =~ /rbbt\/resource\/path\.rb/ or
      l =~ /rbbt\/persist.rb/ or
      l =~ /rbbt\/util\/misc\.rb/ or
      l =~ /progress-monitor\.rb/ 
    }.first.sub(/\.rb[^\w].*/,'.rb') if file.nil?

    relative_to = [relative_to] unless Array === relative_to
    file = File.expand_path(file)
    return Path.setup(file) if relative_to.select{|d| File.exist? File.join(file, d)}.any?

    while file != '/'
      dir = File.dirname file

      return dir if relative_to.select{|d| File.exist? File.join(dir, d)}.any?

      file = File.dirname file
    end

    return nil
  end

  SLASH = "/"[0]
  DOT = "."[0]
  def located?
    self.byte(0) == SLASH || (self.byte(0) == DOT && self.byte(1) == SLASH) || (resource != Rbbt && (Open.remote?(self) || Open.ssh?(self)))
  end
end

module Resource
  def set_software_env(software_dir)
    software_dir.opt.find_all.collect{|d| d.annotate(File.dirname(d)) }.reverse.each do |software_dir|
      next unless software_dir.exists?
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
    @rake_dirs.reject{|dir, content|
      !Misc.common_path(dir, path)
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

    rake_dir = rake_dir.find(:user) if rake_dir.respond_to? :find

    begin
      require 'rbbt/resource/rake'
      Rake.run(rakefile, rake_dir, task)
    rescue Rake::TaskNotFound
      raise $! if rake_dir.nil? or rake_dir.empty? or rake_dir == "/" or rake_dir == "./"
      task = File.join(File.basename(rake_dir), task)
      rake_dir = File.dirname(rake_dir)
      retry
    end
  end
end


