module TSV

  def pthrough(num_threads = 100, new_key_field = nil, new_fields = nil, uniq = false, zipped = false)
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

  def _pthrough(num_threads = 1, new_key_field = nil, new_fields = nil, uniq = false, zipped = false, &block)
    through(new_key_field, new_fields, uniq, zipped, &block) 
  end
end
