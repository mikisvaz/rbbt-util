module RbbtPython
  def self.py2ruby_a(array)
    PyCall::List.(array).to_a
  end

  class << self
    alias to_a py2ruby_a 
  end


  def self.tsv2df(tsv)
    df = nil
    RbbtPython.run 'pandas' do
      df = pandas.DataFrame.new(tsv.values, columns: tsv.fields, index: tsv.keys)
      df.columns.name = tsv.key_field
    end
    df
  end

  def self.df2tsv(tuple, options = {})
    options = Misc.add_defaults options, :type => :list
    IndiferentHash.setup options
    tsv = TSV.setup({}, options)
    tsv.key_field = options[:key_field] || tuple.columns.name
    tsv.fields = py2ruby_a(tuple.columns.values)
    keys = tuple.index.values
    PyCall.len(tuple.index).times do |i|
      k = keys[i]
      tsv[k] = py2ruby_a(tuple.values[i])
    end
    tsv
  end

  def self.list2ruby(list)
    return list unless PyCall::List === list 
    list.collect do |e|
      list2ruby(e)
    end
  end

  def self.numpy2ruby(numpy)
    list2ruby(numpy.tolist)
  end

  def self.obj2hash(obj)
    hash = {}
    RbbtPython.iterate obj.keys do |k|
      hash[k] = obj[k]
    end
    hash
  end

end
