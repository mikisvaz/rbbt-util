module DataModule

  def self.extended(base)
    if defined? base::PKG and base::PKG
      @@mod = base::PKG
    else
      @@mod = Rbbt
    end

    base.module_eval{ @@sharedir = PKGData.get_caller_sharedir}
  end

  alias old_method_missing method_missing
  def method_missing(name, *args, &block)
    if args.any?
      filename = File.join(self.to_s, args.first, name.to_s)
    else
      filename = File.join(self.to_s, name.to_s)
    end

    begin
      @@mod.add_datafiles filename => ['', self.to_s, @@sharedir]
    rescue RuntimeError
      Log.debug $!.message
      old_method_missing name, *args, &block
    end

    case
    when (not File.exists? @@mod.find_datafile(filename))
      nil
    when (TSV.headers @@mod.find_datafile(filename))
      TSV.new(@@mod.find_datafile(filename))
    else
      Open.read(@@mod.find_datafile(filename))
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
end
