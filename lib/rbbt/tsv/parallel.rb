require 'rbbt/util/concurrency'

module TSV

  def pthrough(num_threads = 10, new_key_field = nil, new_fields = nil, uniq = false, zipped = false, &block)
    q = RbbtThreadQueue.new num_threads

    q.init(true, &block)

    begin
      res = through(new_key_field, new_fields, uniq, zipped) do |*p|
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

    q.init(&block)

    begin
      res = through(new_key_field, new_fields, uniq, zipped) do |*p|
        q.process p
      end
      q.join
    ensure
      q.clean
    end

    res
  end

  def _pthrough(num_threads = 1, new_key_field = nil, new_fields = nil, uniq = false, zipped = false, &block)
    through(new_key_field, new_fields, uniq, zipped, &block) 
  end
end
