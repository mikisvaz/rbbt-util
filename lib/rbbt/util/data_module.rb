module DataModule

  def self.extended(base)
    if defined? base::PKG and base::PKG
      base.pkg_module = base::PKG
    else
      base.pkg_module = Rbbt
    end

    base.sharedir = PKGData.get_caller_sharedir 
  end

  def pkg_module
    @pkg_module
  end

  def pkg_module=(pkg_module)
    @pkg_module = pkg_module
  end

  def sharedir
    @sharedir
  end

  def sharedir=(sharedir)
    @sharedir = sharedir
  end

  alias old_method_missing method_missing
  def method_missing(name, *args, &block)
    if args.any?
      filename = File.join(self.to_s, args.first, name.to_s)
    else
      filename = File.join(self.to_s, name.to_s)
    end

    begin
      pkg_module.add_datafiles filename => ['', self.to_s, sharedir]
    rescue 
      Log.debug $!.message
      old_method_missing name, *args, &block
    end

    pkg_module.find_datafile filename
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
end
