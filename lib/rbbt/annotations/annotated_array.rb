module AnnotatedArray
  attr_accessor :list_id

  def to_a
    self.collect{|i| i }
  end

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

    value = value.dup if value.frozen? and (String === value or Array === value)

    value = annotate(value)

    value.extend AnnotatedArray if Array === value and Annotated === value

    if value.respond_to? :container
      value.container       = self
      value.container_index = pos
    end

    value
  end

  def each(&block)

    pos = 0
    super do |value|

      case value
      when Array
        value = value.dup if value.frozen?

        value = annotate(value)

        value.extend AnnotatedArray if Array === value and Annotated === value

        value.container       = self
        value.container_index = pos

        pos += 1

        block.call value
      when String

        value = value.dup if value.frozen?

        value = annotate(value)

        value.container       = self
        value.container_index = pos

        pos += 1

        block.call value
      else
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


  def reject
    res = []

    each do |value|
      res << value unless yield(value)
    end

    annotate(res)
    res.extend AnnotatedArray

    res
  end

  def select(func_name = nil)
    res = []

    if func_name
      each do |elem|
        value = elem.send(func_name)
        if block_given?
          res << elem if yield(value)
        else
          res << elem if value
        end
      end
    else
      each do |elem|
        res << elem if yield(elem)
      end
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


