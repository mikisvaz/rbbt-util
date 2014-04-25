module TSV
  def self.obj_stream(obj)
    case obj
    when IO, File
      obj
    when TSV::Dumper
      obj.stream
    when TSV::Parser
      obj.stream
    end
  end

  def self.stream_name(obj)
    filename_obj   = obj.respond_to?(:filename) ? obj.filename : nil
    filename_obj ||= obj.respond_to?(:path) ? obj.path : nil
    stream_obj = obj_stream(obj)
    filename_obj.nil? ? stream_obj.inspect : filename_obj + "(#{stream_obj.inspect})"
  end

  def self.report(msg, obj, into)
    into = into[:into] if Hash === into and into.include? :into

    Log.error "#{ msg } #{stream_name(obj)} -> #{stream_name(into)}"
  end

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

  def self.traverse_io_array(io, options = {}, &block)
    callback = Misc.process_options options, :callback
    if callback
      while line = io.gets
        res = yield line.strip
        callback.call res
      end
    else
      while line = io.gets
        yield line.strip
      end
    end
  end

  def self.traverse_io(io, options = {}, &block)
    filename = io.filename if io.respond_to? :filename
    callback = Misc.process_options options, :callback
    begin
      if callback
        TSV::Parser.traverse(io, options) do |k,v|
          res = yield k, v
          callback.call res
        end
      else
        TSV::Parser.traverse(io, options, &block)
      end
    rescue
      Log.error "Traverse IO error"
      raise $!
    end
  end

  def self.traverse_obj(obj, options = {}, &block)
    filename = obj.filename if obj.respond_to? :filename
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
    when IO, File
      begin
        if options[:type] == :array
          traverse_io_array(obj, options, &block)
        else
          traverse_io(obj, options, &block)
        end
      rescue Exception
        obj.abort if obj.respond_to? :abort
        raise $!
      ensure
        obj.close if obj.respond_to? :close and not obj.closed?
        obj.join if obj.respond_to? :join
      end
    when Path
      obj.open do |stream|
        traverse_obj(stream, options, &block)
      end
    when TSV::Dumper
      traverse_obj(obj.stream, options, &block)
    when (defined? Step and Step)

      stream = obj.get_stream

      if stream
        traverse_obj(stream, options, &block)
      else
        obj.join
        traverse_obj(obj.path, options, &block)
      end
    when Array
      traverse_array(obj, options, &block)
    when String
      if Open.remote? obj or Misc.is_filename? obj
        Open.open(obj) do |s|
          traverse_obj(s, options, &block)
        end
      else
        raise "Can not open obj for traversal #{Misc.fingerprint obj}"
      end
    when nil
      raise "Can not traverse nil object into #{stream_name(options)}"
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
        res = yield k, v, mutex
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
    begin
      filename = obj.respond_to?(:filename)? obj.filename : "none"
      callback, cleanup = Misc.process_options options, :callback, :cleanup
      q = RbbtProcessQueue.new num, cleanup

      q.callback &callback
      q.init &block

      thread = Thread.new do 
        traverse_obj(obj, options) do |*p|
          q.process *p
        end
      end

      thread.join
    rescue Exception
      Log.error "Exception traversing in cpus: #{$!.message}"
      Log.exception $!

      stream = obj_stream(obj)
      stream.abort if stream.respond_to? :abort
      stream = obj_stream(options[:into])
      stream.abort if stream.respond_to? :abort
      q.abort
      raise $!
    ensure
      q.join
    end
  end

  def self.store_into(store, value)
    begin
      case store
      when Hash
        return false if value.nil?
        if Hash === value
          if TSV === store and store.type == :double
            store.merge_zip value
          else
            store.merge! value
          end
        else
          k,v = value
          store[k] = v
        end
      when TSV::Dumper
        return false if value.nil?
        store.add *value
      when IO
        return false if value.nil?
        value.strip!
        store.puts value
      else
        store << value
      end 
      true
    rescue
      raise "Error storing into #{store.inspect}: #{$!.message}"
    end
  end

  def self.get_streams_to_close(obj)
    close_streams = []
    case obj
    when IO, File
      close_streams << obj
    when TSV::Parser
    when TSV::Dumper
      close_streams << obj.result.in_stream
    when (defined? Step and Step)
      obj.mutex.synchronize do
        case obj.result
        when IO
          close_streams << obj.result
        when TSV::Dumper
          close_streams << obj.result.in_stream
        end
      end
      obj.inputs.each do |input|
        close_streams = get_streams_to_close(input) + close_streams
      end
      obj.dependencies.each do |dependency|
        close_streams = get_streams_to_close(dependency) + close_streams
      end
    end 
    close_streams
  end

  def self.traverse_run(obj, threads, cpus, options = {}, &block)
    if ENV["RBBT_NO_MAP_REDUCE"] == "true" or (threads.nil? and cpus.nil?)
      traverse_obj obj, options, &block
    else
      if threads
        traverse_threads threads, obj, options, &block 
      else
        close_streams = Misc.process_options(options, :close_streams) || []
        close_streams = [close_streams] unless Array === close_streams

        close_streams.concat(get_streams_to_close(obj))
        options[:close_streams] = close_streams

        options[:cleanup] = Proc.new do
          close_streams.uniq.each do |s|
            s.close unless s.closed?
          end
        end if close_streams and close_streams.any?

        traverse_cpus cpus, obj, options, &block
      end
    end
  end

  def self.traverse_stream(obj, threads = nil, cpus = nil, options = {}, &block)
    into = options[:into]
    thread = Thread.new(Thread.current) do |parent|
      begin
        traverse_run(obj, threads, cpus, options, &block)
        into.close if into.respond_to? :close
      rescue Exception
        Log.exception $!
        parent.raise $!
        stream = obj_stream(into)
        stream.abort if stream and stream.respond_to? :abort
      end
    end
    ConcurrentStream.setup(obj_stream(into), :threads => thread)
  end

  def self.traverse(obj, options = {}, &block)
    threads = Misc.process_options options, :threads
    cpus = Misc.process_options options, :cpus
    into = options[:into]

    threads = nil if threads and threads.to_i <= 1
    cpus = nil if cpus and cpus.to_i <= 1

    if into == :stream
      sout = Misc.open_pipe false, false do |sin|                                                                                                                                           
        begin
          traverse(obj, options.merge(:into => sin), &block)                                                                                                                                  
        rescue Exception
          sout.abort if sout.respond_to? :abort
          sout.join if sout.respond_to? :join
        end
      end                                                                                                                                                                                   
      return sout
    end

    if into
      options[:callback] = Proc.new do |e|
        begin
          store_into into, e
        rescue Aborted
          Log.error "Traversal info #{stream_name into} aborted"
          raise $!
        rescue Exception
          Log.exception $!
          raise $!
        end
      end

      case into
      when TSV::Dumper, IO
        traverse_stream(obj, threads, cpus, options, &block)
      else
        traverse_run(obj, threads, cpus, options, &block)
        into.close if into.respond_to? :close
      end

      into
    else
      traverse_run(obj, threads, cpus, options, &block)
    end
  end
end
