require 'rbbt/util/open'
require 'rbbt/util/log'

module PKGData
  FILES = {} unless defined? FILES

  def sharedir(file = __FILE__)
    dir = File.expand_path(File.dirname file)
    while not File.exists?(File.join(dir, 'lib'))
      dir = File.dirname(dir)
    end

    File.join(dir, 'share')
  end

  def get_datafile(file, path, get, sharedir)
    Log.log "Getting data file '#{ file }' into '#{ path }'"
    FileUtils.mkdir_p File.dirname(path) unless File.exists?(File.dirname(path))

    case
    when get.nil?
      load File.join(sharedir, 'install', file)
    when (String === get and File.exists?(File.join(sharedir, 'install', get, 'Rakefile')))
      require 'rake'
      load File.join(sharedir, 'install', get, 'Rakefile')
      old_dir = FileUtils.pwd
      FileUtils.mkdir_p path
      FileUtils.chdir path
      Rake::Task[:default].invoke
      FileUtils.chdir old_dir
    when (String === get and File.exists?(File.join(sharedir, get)))
      Open.write(path, Open.open(File.join(sharedir, get)))
    when String === get 
      Open.write(path, Open.open(get, :wget_options => {:pipe => true}, :nocache => true))
    when Proc === get
      Open.write(path, get.call(file, path))
    end
  end

  def add_datafiles(files = {})
    files.each do |file, info|
      subpath, get = info

      path = File.join(datadir, subpath.to_s, file.to_s)

      FILES[file.to_s] = path

      if ! File.exists?(path) or File.directory?(path)
        sharedir = sharedir(caller[2])
        get_datafile(file, path, get, sharedir)
      end
    end
  end

  def find_datafile(file)
    FILES[file.to_s]
  end
end
