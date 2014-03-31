module TSV

  def self.traverse_tsv(tsv, options = {}, &block)
    callback = Misc.process_options options, :callback

    if callback
      tsv.through options[:key_field], options[:fields] do |k,v|
        callback.call yield(k,v)
      end
    else
      tsv.through options[:key_field], options[:fields] do |k,v|
        yield k,v 
      end
    end
  end

  def self.traverse_hash(hash, options = {}, &block)
    callback = Misc.process_options options, :callback

    if callback
      hash.each do |k,v|
        callback.call yield(k,v)
      end
    else
      hash.each do |k,v|
        yield k,v 
      end
    end
  end

  def self.traverse_array(array, options = {}, &block)
    callback = Misc.process_options options, :callback

    if callback
      array.each do |e|
        res = yield(e)
        callback.call res
      end
    else
      array.each do |e|
        yield e
      end
    end
  end

  def self.traverse_obj(obj, options = {}, &block)
    if options[:type] == :keys
      options[:fields] = []
      options[:type] = :single
    end

    case obj
    when TSV
      traverse_tsv(obj, options, &block)
    when Hash
      traverse_hash(obj, options, &block)
    when TSV::Parser
      callback = Misc.process_options options, :callback
      if callback
        obj.traverse(options) do |k,v|
          res = yield k, v
          callback.call res
        end
      else
        obj.traverse(options, &block)
      end
    when (options[:type] == :array and IO)
      callback = Misc.process_options options, :callback
      if callback
        while not obj.eof?
          res = yield obj.gets.strip
          callback.call res
        end
      else
        while not obj.eof?
          yield obj.gets.strip
        end
      end
    when IO, File
      callback = Misc.process_options options, :callback
      if callback
        TSV::Parser.traverse(obj, options) do |k,v|
          res = yield k, v
          callback.call res
        end
      else
        TSV::Parser.traverse(obj, options, &block)
      end
    when Path
      obj.open do |stream|
        traverse_obj(stream, options, &block)
      end
    when (defined? Step and Step)
      case obj.result
      when IO
        traverse_obj(obj.result, options, &block)
      when TSV::Dumper
        traverse_obj(obj.stream, options, &block)
      else
        obj.join
        traverse_obj(obj.path.open, options, &block)
      end
    when Array
      traverse_array(obj, options, &block)
    when nil
      raise "Can not traverse nil object"
    else
      raise "Unknown object for traversal: #{Misc.fingerprint obj }"
    end
  end

  def self.traverse_threads(num, obj, options, &block)
    callback = Misc.process_options options, :callback

    q = RbbtThreadQueue.new num

    if callback
      block = Proc.new do |k,v,mutex|
        v, mutex = nil, v if mutex.nil?
        res = yield k, v
        mutex.synchronize do
          callback.call res
        end
      end
    end

    q.init true, &block

    traverse_obj(obj, options) do |*p|
      q.process p
    end

    q.join
    q.clean
    nil
  end

  def self.traverse_cpus(num, obj, options, &block)
    callback = Misc.process_options options, :callback

    q = RbbtProcessQueue.new num

    q.callback &callback
    q.init &block

    traverse_obj(obj, options) do |*p|
      q.process *p
    end

    q.join
    q.clean
    nil
  end

  def self.store_into(obj, value)
    case obj
    when Hash
      return if value.nil?
      if Hash === value
        if TSV === obj and obj.type == :double
          obj.merge_zip value
        else
          obj.merge! value
        end
      else
        k,v = value
        obj[k] = v
      end
    when TSV::Dumper
      return if value.nil?
      obj.add *value
    when IO, StringIO
      return if value.nil?
      obj.puts value
    else
      obj << value
    end 
  end

  def self.traverse_run(obj, threads, cpus, options = {}, &block)
    if threads.nil? and cpus.nil? 
      traverse_obj obj, options, &block
    else
      if threads
        traverse_threads threads, obj, options, &block 
      else
        traverse_cpus cpus, obj, options, &block
      end
    end
  end

  def self.traverse(obj, options = {}, &block)
    threads = Misc.process_options options, :threads
    cpus = Misc.process_options options, :cpus
    into = options[:into]

    threads = nil if threads and threads.to_i <= 1
    cpus = nil if cpus and cpus.to_i <= 1

    if into
      callback = Proc.new do |e|
        store_into into, e
      end
      options[:callback] = callback

      case into
      when TSV::Dumper, IO, StringIO
        Thread.new(Thread.current) do |parent|
          begin
            traverse_run(obj, threads, cpus, options, &block)
            into.close 
          rescue Exception
            parent.raise $!
          end
        end
      else
        traverse_run(obj, threads, cpus, options, &block)
      end

      into
    else
      traverse_run(obj, threads, cpus, options, &block)
    end
  end
end
