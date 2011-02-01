module DataModule

  attr_accessor :sharedir, :rakefile, :pkg_module
  def self.extended(base)
    if defined? base::PKG and base::PKG
      base.pkg_module = base::PKG
    else
      base.pkg_module = Rbbt
    end

    base.sharedir = PKGData.get_caller_sharedir 
    
    Dir.glob(File.join(base.sharedir, 'install', base.to_s, '**','Rakefile')).each do |rakefile|
      RakeHelper.files(rakefile).each do |file|
        base.pkg_module.claim file, 
          rakefile.sub(/^#{Regexp.quote File.join(base.sharedir)}\/?/,''), 
          File.dirname(rakefile).sub(/^#{Regexp.quote File.join(base.sharedir, 'install')}\/?/,''),
          base.to_s
      end
    end
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
