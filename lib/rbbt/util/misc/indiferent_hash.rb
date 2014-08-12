module IndiferentHash

  def self.setup(hash)
    hash.extend IndiferentHash 
  end

  def merge(other)
    new = self.dup
    IndiferentHash.setup(new)
    other.each do |k,value|
      new.delete k
      new[k] = value
    end
    new
  end

  def []=(key,value)
    delete(key)
    super(key,value)
  end

  def [](key)
    res = super(key) 
    return res unless res.nil?

    case key
    when Symbol, Module
      super(key.to_s)
    when String
      super(key.to_sym)
    else
      super(key)
    end
  end

  def values_at(*key_list)
    key_list.inject([]){|acc,key| acc << self[key]}
  end

  def include?(key)
    case key
    when Symbol, Module
      super(key) || super(key.to_s)
    when String
      super(key) || super(key.to_sym)
    else
      super(key)
    end
  end

  def delete(key)
    case key
    when Symbol, Module
      super(key) || super(key.to_s)
    when String
      super(key) || super(key.to_sym)
    else
      super(key)
    end
  end
end
