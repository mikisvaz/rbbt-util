require 'rbbt/util/open'
require 'rbbt/util/tsv'
require 'rbbt/util/log'
require 'rake'

module PKGData
  FILES = {} unless defined? FILES
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
      next if line =~ /\/data_module\.rb/ 
        begin
          return PKGData.sharedir_for_file(line)
        rescue SharedirNotFoundError
        end
    end
    raise SharedirNotFoundError
  end

  def run_rake(path, dir, task = nil)
    rakefile = File.join(dir, 'Rakefile')
    return nil unless File.exists? rakefile
    if task.nil?
      task ||= :default
    else
      task.sub!(/\/$/,'') if String === task
      path = File.dirname(path)
    end

    load rakefile
    old_dir = FileUtils.pwd
    begin
      FileUtils.mkdir_p path
      FileUtils.chdir path
      Rake::Task[task].invoke
      Rake::Task[task].reenable
    ensure
      FileUtils.chdir old_dir
    end
    true
  end

  def get_datafile(file, path, get, sharedir)
    Log.log "Getting data file '#{ file }' into '#{ path }'. Get: #{get.to_s}"

    FileUtils.mkdir_p File.dirname(path) unless File.exists?(File.dirname(path))

    case
    when get.nil?
      load File.join(sharedir, 'install', file)

    when Proc === get
      Open.write(path, get.call(file, path))

    when TSV === get
      Open.write(path, get.to_s)

    when String === get
      install_dir =File.expand_path(File.join(sharedir, 'install')) 
      rake_dir  = File.join(install_dir, File.dirname(get), file)
      rake_task = nil

      until rake_dir == install_dir
        return if run_rake(path, rake_dir, rake_task)
        rake_task = File.join(File.basename(rake_dir), rake_task || "")
        rake_dir  = File.dirname(rake_dir)
      end

      if (File.exists?(File.join(sharedir, get)) and not File.directory?(File.join(sharedir, get)))
        Open.write(path, Open.open(File.join(sharedir, get)))
      else
        Open.write(path, Open.open(get, :wget_options => {:pipe => true}, :nocache => true))
      end
    end
  end

  def add_datafiles(files = {})
    files.each do |file, info|
      subpath, get, sharedir = info

      path = File.join(datadir, subpath.to_s, file.to_s)

      FILES[file.to_s] = path

      if not File.exists?(path)
        sharedir ||= PKGData.get_caller_sharedir
        get_datafile(file.to_s, path, get, sharedir)
      end
    end
  end

  def find_datafile(file)
    FILES[file.to_s]
  end
end
