require 'rbbt/resource/rake'

module Path

  def self.caller_lib_dir(file = nil, relative_to = 'lib')
    file = caller.reject{|l| 
      l =~ /rbbt\/(?:resource\.rb|workflow\.rb)/ or
      l =~ /rbbt\/resource\/path\.rb/ or
      l =~ /rbbt\/util\/misc\.rb/ or
      l =~ /progress-monitor\.rb/ 
    }.first.sub(/\.rb[^\w].*/,'.rb') if file.nil?

    file = File.expand_path file
    return Path.setup(file) if File.exists? File.join(file, relative_to)

    while file != '/'
      dir = File.dirname file
      return Path.setup(dir) if File.exists? File.join(dir, relative_to)
      file = File.dirname file
    end

    return nil
  end

  SLASH = "/"[0]
  DOT = "."[0]
  def located?
    self.byte(0) == SLASH or (self.byte(0) == DOT and self.byte(1) == SLASH)
  end
end

module Resource
  def set_software_env(software_dir)
    bin_dir = File.join(software_dir, 'bin')
    opt_dir = File.join(software_dir, 'opt')

    Misc.env_add 'PATH', bin_dir

    FileUtils.mkdir_p opt_dir unless File.exists? opt_dir
    %w(.ld-paths .pkgconfig-paths .aclocal-paths .java-classpaths).each do |file|
      filename = File.join(opt_dir, file)
      FileUtils.touch filename unless File.exists? filename
    end

    if not File.exists? File.join(opt_dir,'.post_install')
      Open.write(File.join(opt_dir,'.post_install'),"#!/bin/bash\n")
    end

    Open.read(File.join opt_dir, '.ld-paths').split(/\n/).each do |line|
      Misc.env_add('LD_LIBRARY_PATH',line.chomp)
      Misc.env_add('LD_RUN_PATH',line.chomp)
    end

    Open.read(File.join opt_dir, '.pkgconfig-paths').split(/\n/).each do |line|
      Misc.env_add('PKG_CONFIG_PATH',line.chomp)
    end

    Open.read(File.join opt_dir, '.ld-paths').split(/\n/).each do |line|
      Misc.env_add('LD_LIBRARY_PATH',line.chomp)
    end

    Open.read(File.join opt_dir, '.ld-paths').split(/\n/).each do |line|
      Misc.env_add('LD_LIBRARY_PATH',line.chomp)
    end

    Open.read(File.join opt_dir, '.aclocal-paths').split(/\n/).each do |line|
      Misc.env_add('ACLOCAL_FLAGS', "-I#{File.join(opt_dir, line.chomp)}", ' ')
    end

    Open.read(File.join opt_dir, '.java-classpaths').split(/\n/).each do |line|
      Misc.env_add('CLASSPATH', "#{File.join(opt_dir,'java', 'lib', line.chomp)}")
    end

    Dir.glob(File.join opt_dir, 'jars', '*').each do |file|
      Misc.env_add('CLASSPATH', "#{File.expand_path(file)}")
    end

    File.chmod 0774, File.join(opt_dir, '.post_install')

    CMD.cmd(File.join(opt_dir, '.post_install'))
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

    rake_dir = rake_dir.find if rake_dir.respond_to? :find

    begin
      Rake.run(rakefile, rake_dir, task)
    rescue Rake::TaskNotFound
      raise $! if rake_dir.nil? or rake_dir.empty? or rake_dir == "/" or rake_dir == "./"
      task = File.join(File.basename(rake_dir), task)
      rake_dir = File.dirname(rake_dir)
      retry
    end
  end
end


