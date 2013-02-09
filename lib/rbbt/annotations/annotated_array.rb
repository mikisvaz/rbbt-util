module AnnotatedArray
  attr_accessor :list_id

  def double_array
    AnnotatedArray === self.send(:[], 0, true)
  end

  def first
    self[0]
  end

  def last
    self[-1]
  end

  def [](pos, clean = false)

    value = super(pos)
    return value if value.nil? or clean

    value = value.dup if value.frozen?

    value = annotate(value)

    value.extend AnnotatedArray if Array === value

    value.container       = self
    value.container_index = pos

    value
  end

  def each(&block)

    pos = 0
    super do |value|

      if value.nil?

        block.call value
      else

        value = value.dup if value.frozen?

        value = annotate(value)

        value.extend AnnotatedArray if Array === value

        value.container       = self
        value.container_index = pos

        pos += 1

        block.call value
      end
    end
  end

  def collect(&block)

    if block_given?

      res = []
      each do |value|
        res << yield(value)
      end

      res
    else

      res = []
      each do |value|
        res << value
      end

      res
    end
  end

  def select(method = nil, *args)

    if method

      res = self.zip( self.send(method, *args) ).
        select{|e,result| result }. 
        collect{|element,r| element }
    else

      return self unless block_given?

      res = []
      each do |value|
        res << value if yield(value)
      end
    end

    annotate(res)
    res.extend AnnotatedArray

    res
  end

  def reject
    res = []

    each do |value|
      res << value unless yield(value)
    end

    annotate(res)
    res.extend AnnotatedArray

    res
  end

  def select
    res = []

    each do |value|
      res << value if yield(value)
    end

    annotate(res)
    res.extend AnnotatedArray

    res
  end

  def subset(list)

    res = (self & list)

    annotate(res)
    res.extend AnnotatedArray

    res
  end

  def remove(list)

    res = (self - list)

    annotate(res)
    res.extend AnnotatedArray

    res
  end

  def sort(&block)
    res = self.collect.sort(&block).collect{|value| value.respond_to?(:clean_annotations) ? value.clean_annotations.dup : value.dup }

    annotate(res)
    res.extend AnnotatedArray

    res
  end

  def select_by(method, *args, &block)
    return [] if self.empty? and not self.respond_to? :annotate

    values = self.send(method, *args)
    values = values.clean_annotations if values.respond_to? :clean_annotations

    new = []
    if block_given?
      self.clean_annotations.each_with_index do |e,i|
        new << e if yield(values[i])
      end
    else
      self.clean_annotations.each_with_index do |e,i|
        new << e if values[i]
      end
    end
    self.annotate(new)
    new.extend AnnotatedArray

    new
  end

  %w(compact uniq flatten reverse sort_by).each do |method|

    self.module_eval <<-EOC

      def #{method}
        res = super

        annotate(res)
        res.extend AnnotatedArray

        res
      end

      EOC
  end
end


