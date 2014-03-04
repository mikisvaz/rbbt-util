require 'rbbt/util/concurrency'

module TSV

  def pthrough_old(num_threads = 10, new_key_field = nil, new_fields = nil, uniq = false, zipped = false)
    q = Queue.new
    mutex = Mutex.new

    threads = []

    done = false
    num_threads.times do |i|
      threads << Thread.new(Thread.current) do |current|
        begin
          loop do
            p = q.pop
            p << mutex
            yield *p
            next if q.length == 0 and done
          end
        rescue Exception
          current.raise $!
        end
      end
    end

    max = 10_000_000
    res = through(new_key_field, new_fields, uniq, zipped) do |*p|
      if q.length >= max
        Thread.pass
        q << p
      end
      q << p
    end

    done == true

    Thread.pass while q.length > 0


    threads.each{|t| t.kill }

    res
  end

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

  def ppthrough(num_procs = 3, new_key_field = nil, new_fields = nil, uniq = false, zipped = false, &block)

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
