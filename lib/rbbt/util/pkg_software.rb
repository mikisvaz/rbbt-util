require 'rbbt/util/open'
require 'rbbt/util/tsv'
require 'rbbt/util/log'
require 'rbbt/util/cmd'
require 'rake'

module PKGSoftware
  SOFTWARE = {} unless defined? SOFTWARE

  class SharedirNotFoundError < StandardError; end

  def self.sharedir_for_file(file = __FILE__)
    dir = File.expand_path(File.dirname file)

    while not File.exists?(File.join(dir, 'lib')) and dir != '/'
      dir = File.dirname(dir)
    end

    if File.exists? File.join(dir, 'lib')
      File.join(dir, 'share')
    else
      raise SharedirNotFoundError
    end
  end

  def self.get_caller_sharedir
    caller.each do |line|
      next if line =~ /\/data_module\.rb/  or line =~ /\/pkg_data\.rb/ 
        begin
          return PKGData.sharedir_for_file(line)
        rescue SharedirNotFoundError
        end
    end
    raise SharedirNotFoundError
  end

  def software_dir
    File.join(datadir, 'software')
  end

  def opt_dir
    File.join(software_dir, 'opt')
  end

  def bin_dir
    File.join(opt_dir, 'bin')
  end


  def get_pkg(pkg, path, get, sharedir)
    Log.log "Getting software '#{ pkg }' into '#{ path }'. Get: #{get.to_s}"

    FileUtils.mkdir_p File.dirname(path) unless File.exists?(File.dirname(path))

    if get.nil? or get.empty?
      CMD.cmd("#{File.join(sharedir, 'install', 'software', pkg)} #{File.join(Rbbt.rootdir, 'share/install/software/lib', 'install_helpers')} #{software_dir}", :stderr => Log::HIGH)
    else 
      CMD.cmd("#{File.join(sharedir, 'install', 'software', get)} #{File.join(Rbbt.rootdir, 'share/install/software/lib', 'install_helpers')} #{software_dir}")
    end
  end

  def add_software(pkgs = {})
    pkgs.each do |pkg, info|
      subpath, get, datadir = info

      setup_env(software_dir)

      path = File.join(opt_dir, subpath.to_s, pkg.to_s)

      SOFTWARE[file.to_s] = path

      if not File.exists?(path)
        sharedir ||= PKGSoftware.get_caller_sharedir
        get_pkg(pkg.to_s, path, get, sharedir)
      end
    end
  end

  def find_software(pkg)
    SOFTWARE[pkg.to_s]
  end


  def setup_env(software_dir)
    Misc.env_add 'PATH', bin_dir

    %w(.ld-paths .pkgconfig-paths .aclocal-paths .java-classpaths .post_install).each do |file|
      filename = File.join(opt_dir, file)
      FileUtils.touch filename unless File.exists? filename
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
      Misc.env_add('ACLOCAL_FLAGS', "-I#{File.join(opt_dir, line.chomp)}")
    end

    Open.read(File.join opt_dir, '.java-classpaths').split(/\n/).each do |line|
      Misc.env_add('CLASSPATH', "-I#{File.join(opt_dir,'java', 'lib', line.chomp)}")
    end

    File.chmod 0774, File.join(opt_dir, '.post_install')
    CMD.cmd(File.join(opt_dir, '.post_install'))
  end

 # def run_rake(path, dir, task = nil)
 #   rakefile = File.join(dir, 'Rakefile')
 #   return nil unless File.exists? rakefile
 #   if task.nil?
 #     task ||= :default
 #   else
 #     task.sub!(/\/$/,'') if String === task
 #     path = File.dirname(path)
 #   end

 #   load rakefile
 #   old_dir = FileUtils.pwd
 #   begin
 #     FileUtils.mkdir_p path
 #     FileUtils.chdir path
 #     Rake::Task[task].invoke
 #     Rake::Task[task].reenable
 #   ensure
 #     FileUtils.chdir old_dir
 #   end
 #   true
 # end

 # def get_datafile(file, path, get, sharedir)
 #   Log.log "Getting data file '#{ file }' into '#{ path }'. Get: #{get.to_s}"

 #   FileUtils.mkdir_p File.dirname(path) unless File.exists?(File.dirname(path))

 #   case
 #   when get.nil?
 #     load File.join(sharedir, 'install', file)

 #   when Proc === get
 #     Open.write(path, get.call(file, path))

 #   when TSV === get
 #     Open.write(path, get.to_s)

 #   when String === get
 #     install_dir =File.expand_path(File.join(sharedir, 'install')) 
 #     rake_dir  = File.join(install_dir, File.dirname(get), file)
 #     rake_task = nil

 #     until rake_dir == install_dir
 #       return if run_rake(path, rake_dir, rake_task)
 #       rake_task = File.join(File.basename(rake_dir), rake_task || "")
 #       rake_dir  = File.dirname(rake_dir)
 #     end

 #     if (File.exists?(File.join(sharedir, get)) and not File.directory?(File.join(sharedir, get)))
 #       Open.write(path, Open.open(File.join(sharedir, get)))
 #     else
 #       Open.write(path, Open.open(get, :wget_options => {:pipe => true}, :nocache => true))
 #     end
 #   end
 # end

 # def add_datafiles(files = {})
 #   files.each do |file, info|
 #     subpath, get, sharedir = info

 #     path = File.join(datadir, subpath.to_s, file.to_s)

 #     FILES[file.to_s] = path

 #     if not File.exists?(path)
 #       sharedir ||= PKGData.get_caller_sharedir
 #       get_datafile(file.to_s, path, get, sharedir)
 #     end
 #   end
 # end

 # def find_datafile(file)
 #   FILES[file.to_s]
 # end
end
