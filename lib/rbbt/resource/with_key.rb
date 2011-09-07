module Resource
  module WithKey
    def self.extended(base)
      class << base
        attr_accessor :klass, :key
      end
    end

    alias :old_method_missing :method_missing
    def method_missing(name, *args)
      return old_method_missing(name, *args) if name.to_s =~ /^to_/
      if key
        klass.send(name, key, *args)
      else
        klass.send(name, *args)
      end
    end
  end

  def with_key(key)
    klass = self
    o     = Object.new
    o.extend WithKey
    o.klass = self
    o.key   = key
    o
  end
end
