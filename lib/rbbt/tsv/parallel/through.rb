require 'scout/tsv'
module TSV

  def pthrough(num_threads = 10, new_key_field = nil, new_fields = nil, uniq = false, zipped = false, &block)
    q = RbbtThreadQueue.new num_threads

    q.init(true, &block)

    begin
      res = through(new_key_field, new_fields, one2one: zipped) do |*p|
        q.process p
      end
      q.join
    ensure
      q.clean
    end

  end

  def ppthrough_callback(&block)
    @ppthrough_callback = block
  end

  def ppthrough(num_procs = 7, new_key_field = nil, new_fields = nil, uniq = false, zipped = false, &block)

    q = RbbtProcessQueue.new num_procs

    q.callback &@ppthrough_callback
    @ppthrough_callback = nil

    q.init do |k,v|
      block.call k,v
    end

    begin
      res = through(new_key_field, new_fields, uniq, zipped) do |*p|
        q.process q
      end
      q.join
    ensure
      q.clean
    end

    res
  end

  def ppthrough(num_procs = 7, new_key_field = nil, new_fields = nil, uniq = false, zipped = false, &block)

    q = RbbtProcessQueue.new num_procs

    q.callback &@ppthrough_callback
    @ppthrough_callback = nil

    _pat_size = 20
    _pat = "A" << _pat_size.to_s

    num_fields = fields.length
    pattern = case type
              when :single, :flat
                _pat * 2
              when :list, :double
                _pat * (num_fields + 1)
              end

    q.init do |str|
      _parts = str.unpack(pattern)

      case type
      when :single
        k, v = _parts
      when :list
        k, *v = _parts
      when :flat
        k, v = _parts
        v = v.split "|"
      when :double
        k, *v = _parts
        v = v.collect{|l| l.split "|" }
      end

      block.call k,v
    end

    begin
      res = through(new_key_field, new_fields, uniq, zipped) do |k,v|
        case type
        when :flat
          v = v * "|" 
        when :double
          v = v.collect{|l| l * "|" } if type == :double
        end

        str = [k,v].flatten.pack(pattern)
        q.process str
      end
      q.join
    ensure
      q.clean
    end

    res
  end
end
