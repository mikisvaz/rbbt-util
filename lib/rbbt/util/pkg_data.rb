require 'rbbt/util/open'
require 'rbbt/util/tsv'
require 'rbbt/util/log'
require 'rbbt/util/path'
require 'rbbt/util/rake'

module PKGData
  attr_accessor :claims
  def self.extended(pkg_module)
    pkg_module.claims = {}
  end

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

  def files
    path = datadir.dup.extend Path
    path.pkg_module      = self
    path.datadir         = datadir
    path
  end

  def in_datadir?(file)
    Misc.in_directory? file, datadir
  end

  # file is the complete path of the file inside the datadir
  # get is the get method. :Rakefile for 
  def claim(file, get = nil, subdir = nil, namespace = nil,  sharedir = nil)
    file = case
           when (file.nil? or file === :all)
             File.join(datadir, subdir.to_s)
           when in_datadir?(file)
             file
           else
             File.join(datadir, subdir.to_s, file.to_s)
           end

    sharedir ||= PKGData.get_caller_sharedir
    claims[file] = {:get => get, :subdir => subdir, :sharedir => sharedir, :namespace => namespace}
    produce(file, get, subdir, sharedir) if TSV === get
    produce(file, get, subdir, sharedir) if String === get and not File.exists?(get) and reclaim(file).nil? and not File.basename(get.to_s) == "Rakefile"
  end

  def reclaim(file)
    file = File.expand_path(file.dup)
    return nil unless in_datadir? file

    while file != File.expand_path(datadir)
      if @claims[file]
        return [file, @claims[file]]
      end
      file = File.dirname(file)
    end
    nil
  end

  def declaim(file)
    @claims.delete file if @claims.include? file
  end

  def produce_with_rake(rakefile, subdir, file)
    task  = File.expand_path(file).sub(/^.*#{Regexp.quote(File.join(datadir, subdir))}\/?/, '')
    RakeHelper.run(rakefile, task, File.join(File.join(datadir, subdir)))
  end

  def produce(file, get, subdir, sharedir)
    Log.low "Getting data file '#{ file }' into '#{ subdir }'. Get: #{get.class}"

    FileUtils.mkdir_p File.dirname(file) unless File.exists?(File.dirname(file))

    relative_path = Misc.path_relative_to file, datadir
    case 
    when get.nil?
      FileUtils.cp File.join(sharedir, subdir.to_s, relative_path), file.to_s
    when StringIO === get
      Open.write(file, get.read)
    when Proc === get
      Open.write(file, get.call)
    when TSV === get
      Open.write(file, get.to_s)
    when ((String === get or Symbol === get) and File.basename(get.to_s) == "Rakefile")
      if Symbol === get
        rakefile = File.join(sharedir, subdir, get.to_s)
      else
        rakefile = File.join(sharedir, get.to_s)
      end
      produce_with_rake(rakefile, subdir, file)
    when (String === get and Open.remote? get)
      Open.write(file, Open.read(get, :wget_options => {:pipe => true}, :nocache => true))
    else
      raise "Unknown Get: #{get.class}"
    end
  end
end
