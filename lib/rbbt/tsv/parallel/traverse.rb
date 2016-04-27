module TSV

  def self.obj_stream(obj)
    case obj
    when nil
      nil
    when (defined? Step and Step)
      obj.result
    when IO, File, Zlib::GzipReader, Bgzf
      obj
    when TSV::Dumper
      obj.stream
    when TSV::Parser
      obj.stream
    end
  end

  def self.guess_max(obj)
    begin
      case obj
      when (defined? Step and Step)
        if obj.done?
          CMD.cmd("wc -l '#{obj.path.find}'").read.to_i
        else
          nil
        end
      when TSV
        obj.length
      when Array, Hash
        obj.size
      when File
        return nil if Open.gzip?(obj) or Open.bgzip?(obj)
        CMD.cmd("wc -l '#{obj.path}'").read.to_i
      when Path, String
        obj = obj.find if Path === obj
        if File.exists? obj
          return nil if Open.gzip?(obj) or Open.bgzip?(obj)
          CMD.cmd("wc -l '#{obj}'").read.to_i
        else
          nil
        end
      end
    rescue Exception
      Log.exception $!
      nil
    end
  end

  def self.stream_name(obj)
    return "nil" if obj.nil?
    filename_obj   = obj.respond_to?(:filename) ? obj.filename : nil
    filename_obj ||= obj.respond_to?(:path) ? obj.path : nil
    stream_obj = obj_stream(obj) || obj
    obj.class.to_s << "-" << Misc.fingerprint(stream_obj)
  end

  def self.report(msg, obj, into)
    into = into[:into] if Hash === into and into.include? :into

    Log.medium{"#{ msg } #{stream_name(obj)} -> #{stream_name(into)}"}
  end

  #{{{ TRAVERSE OBJECTS

  def self.traverse_tsv(tsv, options = {}, &block)
    callback, bar, join = Misc.process_options options, :callback, :bar, :join

    if callback
      bar.init if bar
      tsv.through options[:key_field], options[:fields] do |k,v|
        begin
          callback.call yield(k,v)
        ensure
          bar.tick if bar
        end
      end
    else
      bar.init if bar
      tsv.through options[:key_field], options[:fields] do |k,v|
        begin
          yield k,v 
        ensure
          bar.tick if bar
        end
      end
    end
    Log::ProgressBar.remove_bar(bar) if bar
    join.call if join
  end

  def self.traverse_hash(hash, options = {}, &block)
    callback, bar, join = Misc.process_options options, :callback, :bar, :join

    if callback
      hash.each do |k,v|
        begin
          callback.call yield(k,v)
        ensure
          bar.tick if bar
        end
      end
    else
      hash.each do |k,v|
        begin
          yield k,v 
        ensure
          bar.tick if bar
        end
      end
    end
    Log::ProgressBar.remove_bar(bar) if bar
    join.call if join
  end

  def self.traverse_array(array, options = {}, &block)
    callback, bar, join = Misc.process_options options, :callback, :bar, :join

    if callback
      bar.init if bar
      array.each do |e|
        begin
          callback.call yield(e)
        ensure
          bar.tick if bar
        end
      end
    else
      bar.init if bar
      array.each do |e|
        begin
          yield e
        rescue Exception
          Log.exception $!
          raise $!
        ensure
          bar.tick if bar
        end
      end
    end
    Log::ProgressBar.remove_bar(bar) if bar
    join.call if join
  end

  def self.traverse_io_array(io, options = {}, &block)
    callback, bar, join = Misc.process_options options, :callback, :bar, :join
    if File === io and io.closed? 
      begin
        Log.medium{"Rewinding stream #{stream_name(io)}"}
        io.reopen io.filename, "r"
      rescue
        Log.exception $!
        raise "File closed and could not reopen #{stream_name(io)}"
      end
    end

    if callback
      bar.init if bar
      while line = io.gets
        if line[-1] != "\n"
          while c = io.getc
            line << c
            break if c=="\n"
          end
        end
        begin
          callback.call yield line.strip
        ensure
          bar.tick if bar
        end
      end
    else
      bar.init if bar
      while line = io.gets
        begin
          yield line.strip
        ensure
          bar.tick if bar
        end
      end
    end
    Log::ProgressBar.remove_bar(bar) if bar
    join.call if join
  end

  def self.traverse_io(io, options = {}, &block)
    callback, bar, join = Misc.process_options options, :callback, :bar, :join
    if File === io and io.closed? 
      begin
        Log.medium{"Rewinding stream #{stream_name(io)}"}
        io.reopen io.filename, "r"
      rescue
        Log.exception $!
        raise "File closed and could not reopen #{stream_name(io)}"
      end
    end

    if callback
      bar.init if bar
      TSV::Parser.traverse(io, options) do |k,v|
        begin
          callback.call yield k, v
        ensure
          bar.tick if bar
        end
      end
    else
      options[:monitor] = bar
      TSV::Parser.traverse(io, options.merge(:monitor => bar), &block)
    end
    Log::ProgressBar.remove_bar(bar) if bar
    join.call if join
  end

  def self.traverse_obj(obj, options = {}, &block)
    if options[:type] == :keys
      options[:fields] = []
      options[:type] = :single
    end

    Log.medium{"Traversing #{stream_name(obj)} #{Log.color :green, "->"} #{stream_name(options[:into])}"}
    begin
      case obj
      when TSV
        traverse_tsv(obj, options, &block)
      when Hash
        traverse_hash(obj, options, &block)
      when TSV::Parser
        callback = Misc.process_options options, :callback
        if callback
          obj.traverse(options) do |k,v|
            callback.call yield k, v
          end
        else
          obj.traverse(options, &block)
        end
      when IO, File, Zlib::GzipReader, Bgzf, StringIO
        begin
          if options[:type] == :array
            traverse_io_array(obj, options, &block)
          else
            traverse_io(obj, options, &block)
          end
        rescue Aborted
          obj.abort if obj.respond_to? :abort
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
      when Set
        traverse_array(obj.to_a, options, &block)
      when String
        if Open.remote? obj or Misc.is_filename? obj
          Open.open(obj) do |s|
            traverse_obj(s, options, &block)
          end
        else
          raise "Can not open obj for traversal #{Misc.fingerprint obj}"
        end
      when nil
        raise "Can not traverse nil object into #{stream_name(options[:into])}"
      else
        raise "Unknown object for traversal: #{Misc.fingerprint obj }"
      end
    rescue IOError
      Log.medium{"IOError traversing #{stream_name(obj)}: #{$!.message}"}
      stream = obj_stream(obj)
      stream.abort if stream and stream.respond_to? :abort
      stream = obj_stream(options[:into])
      stream.abort if stream.respond_to? :abort
      raise $!
    rescue Errno::EPIPE
      Log.medium{"Pipe closed while traversing #{stream_name(obj)}: #{$!.message}"}
      stream = obj_stream(obj)
      stream.abort if stream and stream.respond_to? :abort
      stream = obj_stream(options[:into])
      stream.abort if stream.respond_to? :abort
      raise $!
    rescue Aborted
      Log.medium{"Aborted traversing #{stream_name(obj)}"}
      stream = obj_stream(obj)
      stream.abort if stream and stream.respond_to? :abort
      stream = obj_stream(options[:into])
      stream.abort if stream.respond_to? :abort
      Log.medium{"Aborted traversing 2 #{stream_name(obj)}"}
    rescue Exception
      Log.medium{"Exception traversing #{stream_name(obj)}"}
      begin
        stream = obj_stream(obj)
        stream.abort if stream and stream.respond_to? :abort
        stream = obj_stream(options[:into])
        stream.abort if stream.respond_to? :abort
      rescue Exception
      ensure
        raise $!
      end
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
      callback, cleanup, join, respawn, bar = Misc.process_options options, :callback, :cleanup, :join, :respawn, :bar
      respawn = true if ENV["RBBT_RESPAWN"] and ENV["RBBT_RESPAWN"] == "true"

      Log.low "Traversing in #{ num } cpus: #{respawn ? "respawn" : "no respawn"}"
      q = RbbtProcessQueue.new num, cleanup, join, respawn
      callback = Proc.new{ bar.tick } if callback.nil? and bar
      q.callback &callback
      q.init &block

      bar.init if bar
      traverse_obj(obj, options) do |*p|
        q.process *p
      end

      q.join

    rescue Interrupt, Aborted
      q.abort
      Log.medium{"Aborted traversal in CPUs for #{stream_name(obj) || Misc.fingerprint(obj)}: #{$!.backtrace*","}"}
      stream = obj_stream(obj)
      stream.abort if stream.respond_to? :abort
      stream = obj_stream(options[:into])
      stream.abort if stream.respond_to? :abort
      raise "Traversal aborted"
    rescue Exception
      q.abort
      Log.medium "Exception during traversal in CPUs for #{stream_name(obj) || Misc.fingerprint(obj)}: #{$!.message}"
      stream = obj_stream(obj)
      stream.abort if stream.respond_to? :abort
      stream = obj_stream(options[:into])
      stream.abort if stream.respond_to? :abort
      raise $!
    ensure
      q.clean
      Log::ProgressBar.remove_bar(bar) if bar
    end
  end

  def self.store_into(store, value)
    if MultipleResult === value
      value.each do |v|
        store_into store, v
      end
      return
    end
    begin
      return false if value.nil?
      case store
      when TSV
        if store.type == :double or store.type == :flat
          case value
          when TSV, Hash
            store.merge_zip value
          else
            store.zip_new *value
          end
        else
          k,v = value
          store[k] = v
        end
      when Hash
        case value
        when TSV, Hash
          store.merge! value 
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
    rescue Aborted, Interrupt
      Log.medium "Aborted storing into #{Misc.fingerprint store}"
      stream = obj_stream(store)
      stream.abort if stream.respond_to? :abort
    rescue Exception
      Log.medium "Exception storing into #{Misc.fingerprint store}: #{$!.message}"
      stream = obj_stream(store)
      stream.abort if stream.respond_to? :abort
      raise $!
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
    threads = nil if threads == 1
    cpus = nil if cpus == 1
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

        if close_streams and close_streams.any?
          options[:cleanup] = Proc.new do
            close_streams.uniq.each do |s|
              s.close unless s.closed?
            end
          end 
        end

        traverse_cpus cpus, obj, options, &block
      end
    end
  end

  def self.traverse_stream(obj, threads = nil, cpus = nil, options = {}, &block)
    into = options[:into]
    thread = Thread.new(Thread.current) do |parent|
      begin
        traverse_run(obj, threads, cpus, options, &block)
        into.close if into.respond_to?(:close) and not (into.respond_to? :closed? and into.closed?)
      rescue Exception
        stream = obj_stream(obj)
        stream.abort if stream and stream.respond_to? :abort
        stream = obj_stream(into)
        stream.abort if stream and stream.respond_to? :abort
        parent.raise $!
        raise $!
      end
    end
    ConcurrentStream.setup(obj_stream(into), :threads => thread)
  end

  def self.traverse(obj, options = {}, &block)
    into = options[:into]

    case into
    when :stream
      sout = Misc.open_pipe false, false do |sin|                                                                                                                                           
        begin
          traverse(obj, options.merge(:into => sin), &block)                                                                                                                                  
        rescue Exception
          sout.abort if sout.respond_to? :abort
          sout.join if sout.respond_to? :join
        end
      end                                                                                                                                                                                   
      return sout
    when :dumper
      obj_options = obj.respond_to?(:options) ? obj.options : {}
      dumper = TSV::Dumper.new obj_options.merge(options)
      dumper.init
      _options = options.merge(obj_options).merge(:into => dumper)
      traverse(obj, _options, &block)                                                                                                                                  
      return dumper
    end

    threads = Misc.process_options options, :threads
    cpus = Misc.process_options options, :cpus
    threads = nil if threads and threads.to_i <= 1
    cpus = nil if cpus and cpus.to_i <= 1

    if options[:keys]
      case options[:keys]
      when TrueClass
        options[:type] = :keys
      when String
        options[:type] = :keys
        options[:key_field] = options[:keys]
        options[:fields] = []
      end
    end

    bar = Misc.process_options options, :bar
    bar ||= Misc.process_options options, :progress
    options[:bar] = case bar
                    when String
                      max = guess_max(obj)
                      Log::ProgressBar.new_bar(max, {:desc => bar}) 
                    when TrueClass
                      max = guess_max(obj)
                      Log::ProgressBar.new_bar(max, nil) 
                    when Fixnum
                      max = guess_max(obj)
                      Log::ProgressBar.new_bar(bar) 
                    when Hash
                      max = Misc.process_options(bar, :max) || max
                      Log::ProgressBar.new_bar(max, bar) 
                    when Log::ProgressBar
                      bar.max ||= guess_max(obj)
                      bar
                    else
                      if (defined? Step and Step === bar)
                        max = guess_max(obj)
                        Log::ProgressBar.new_bar(max, {:desc => bar.status, :file => bar.file(:progress)}) 
                      else
                        bar
                      end
                    end

    if into
      bar = Misc.process_options options, :bar

      options[:join] = Proc.new do
        Log::ProgressBar.remove_bar(bar)
      end if bar

      options[:callback] = Proc.new do |e|
        begin
          store_into into, e
        rescue Aborted
          Log.medium "Aborted callback #{stream_name(obj)} #{Log.color :green, "->"} #{stream_name(options[:into])}"
          stream = nil
          stream = get_stream obj
          stream.abort if stream.respond_to? :abort
          raise $!
        rescue Exception
          Log.medium "Exception callback #{stream_name(obj)} #{Log.color :green, "->"} #{stream_name(options[:into])}"
          stream = nil
          stream = get_stream obj
          stream.abort if stream.respond_to? :abort
          raise $!
        ensure
          bar.tick if bar
        end
      end

      bar.init if bar
      case into
      when TSV::Dumper, IO
        traverse_stream(obj, threads, cpus, options, &block)
      else
        traverse_run(obj, threads, cpus, options, &block)
        into.close if into.respond_to?(:close) and not (into.respond_to? :closed and into.closed?)
      end

      into
    else
      traverse_run(obj, threads, cpus, options, &block)
    end
  end
end
