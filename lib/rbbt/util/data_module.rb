module DataModule

  def self.rakefiles(sharedir, data_module)
    Dir.glob(File.join(sharedir, 'install', data_module.to_s, '**','Rakefile')).collect{|f| File.expand_path f}
  end

  attr_accessor :sharedir, :rakefile, :pkg_module
  def self.extended(data_module)
    if defined? data_module::PKG and data_module::PKG
      data_module.pkg_module = data_module::PKG
    else
      data_module.pkg_module = Rbbt
    end

    data_module.sharedir = PKGData.get_caller_sharedir 
    
    install_dir = File.join(data_module.sharedir, 'install')
    rake_sharedir  = File.join(data_module.sharedir, 'install')

    rakefiles(data_module.sharedir, data_module).each do |rakefile|
      rakefile_dir = File.dirname(rakefile)

      RakeHelper.files(rakefile).each do |file|
        file_path = Misc.path_relative_to(File.join(File.dirname(rakefile), file), rakefile_dir)
        get       = :Rakefile
        subdir    = Misc.path_relative_to(File.dirname(rakefile), install_dir)
        namespace = data_module.to_s.gsub('/', '::')

        data_module.pkg_module.claim file_path, get, subdir, namespace, rake_sharedir
      end
    end
  end

  def files
    DataModule.rakefiles(sharedir, self).collect do |rakefile|
      RakeHelper.files(rakefile).collect
    end.flatten
  end

  def datadir
    File.join(pkg_module.datadir, self.to_s)
  end

  module WithKey
    def klass=(klass)
      @klass = klass
    end
   
    def klass
      @klass
    end

    def key=(key)
      @key = key
    end

    def key
      @key
    end

    def method_missing(name, *args)
      if key
        klass.send(name, key, *args)
      else
        klass.send(name, *args)
      end
    end
  end

  def with_key(key)
    klass = self
    o = Object.new
    o.extend WithKey
    o.klass = self
    o.key = key
    o
  end

  alias old_method_missing method_missing
  def method_missing(name, *args, &block)
    begin
      if args.any?
        pkg_module.files[self.to_s][args.first][name] 
      else
        pkg_module.files[self.to_s][name] 
      end
    rescue
      Log.debug $!.message
      Log.debug $!.backtrace * "\n"
      old_method_missing name, *args, &block
    end
  end
end
