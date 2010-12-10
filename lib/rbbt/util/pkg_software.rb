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


      if not File.exists?(path)
        sharedir ||= PKGSoftware.get_caller_sharedir
        get_pkg(pkg.to_s, path, get, sharedir)
      end

      SOFTWARE[file.to_s] = path
    end
  end

  def find_software(pkg)
    SOFTWARE[pkg.to_s]
  end


  def setup_env(software_dir)
    Misc.env_add 'PATH', bin_dir

    FileUtils.mkdir_p opt_dir unless File.exists? opt_dir
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
end
