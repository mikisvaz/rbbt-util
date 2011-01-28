require 'rbbt/util/open'
require 'rbbt/util/tsv'
require 'rbbt/util/log'
require 'rbbt/util/rake'

module PKGData
  attr_accessor :claims
  def self.extended(base)
    base.claims = {}
  end

  module Path
    attr_accessor :base

    def method_missing(name, *args, &block)
      new = File.join(self.dup, name.to_s)
      new.extend Path
      new.base = base
      new
    end

    def [](name)
      new = File.join(self.dup, name.to_s)
      new.extend Path
      new.base = base
      new
    end

    def tsv(options = {})
      produce
      TSV.new self, options
    end

    def index(field = nil, other = nil, options = {})
      produce
      TSV.index self, options.merge(:field => field, :other => other)
    end

    def open
      produce
      Open.open(self)
    end

    def read
      produce
      Open.read(self)
    end

    def produce
      Log.debug("Base #{ base.inspect }")
      return if File.exists? self

      Log.debug("Trying to produce '#{ self }'")
      file, producer = base.reclaim self
      base.produce(self, producer[:get], producer[:subdir], producer[:sharedir])
    end
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
    path.base = self
    path
  end

  def in_datadir?(file)
    if File.expand_path(file.to_s) =~ /^#{Regexp.quote File.expand_path(datadir)}/
      true
    else
      false
    end
  end

  def claim(file, get = nil, subdir = nil, sharedir = nil)
    file = case
           when (file.nil? or file === :all)
             File.join(datadir, subdir.to_s)
           when in_datadir?(file)
             file
           else
             File.join(datadir, subdir.to_s, file.to_s)
           end

    sharedir ||= PKGData.get_caller_sharedir
    claims[file] = {:get => get, :subdir => subdir, :sharedir => sharedir}
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

    case 
    when get.nil?
      FileUtils.cp File.join(sharedir, subdir.to_s, File.basename(file.to_s)), file.to_s
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
    when String === get
      Open.write(file, Open.read(get, :wget_options => {:pipe => true}, :nocache => true))
    else
      raise "Unknown Get: #{get.class}"
    end
  end
end
