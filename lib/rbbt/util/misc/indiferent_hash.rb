module IndiferentHash

  def self.setup(hash)
    hash.extend IndiferentHash 
  end

  def merge(other)
    new = self.dup
    IndiferentHash.setup(new)
    other.each do |k,value|
      new[k] = value
    end
    new
  end

  def []=(key,value)
    delete(key)
    super(key,value)
  end

  def _default?
    @_default ||= self.default or self.default_proc
  end

  def [](key, *args)
    res = super(key, *args) 
    res = IndiferentHash.setup(res) if Hash === res
    return res unless res.nil? or (_default? and not keys.include? key)

    case key
    when Symbol, Module
      res = super(key.to_s, *args)
    when String
      res = super(key.to_sym, *args)
    end

    res = IndiferentHash.setup(res) if Hash === res

    res
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

  def clean_version
    clean = {}
    each do |k,v|
      clean[k.to_s] = v unless clean.include? k.to_s
    end
    clean
  end
end

module CaseInsensitiveHash
  

  def self.setup(hash)
    hash.extend CaseInsensitiveHash
  end

  def downcase_keys
    @downcase_keys ||= begin
                         down = {} 
                         keys.collect{|key| 
                           down[key.to_s.downcase] = key 
                         }
                         down
                       end
  end

  def [](key, *rest)
    value = super(key, *rest)
    return value unless value.nil?
    key_downcase = key.to_s.downcase
    super(downcase_keys[key_downcase])
  end

  def values_at(*keys)
    keys.collect do |key|
      self[key]
    end
  end
  
end
