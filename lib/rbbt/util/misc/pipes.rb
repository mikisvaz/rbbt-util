require 'rbbt'

module Misc
  class << self
    attr_accessor :sensiblewrite_lock_dir
    
    def sensible_write_locks
      @sensiblewrite_locks ||= Rbbt.tmp.sensiblewrite_locks.find
    end
  end

  class << self
    attr_accessor :sensiblewrite_dir
    def sensiblewrite_dir
      @sensiblewrite_dir = Rbbt.tmp.sensiblewrite.find
    end
  end

  PIPE_MUTEX = Mutex.new

  OPEN_PIPE_IN = []
  def self.pipe
    OPEN_PIPE_IN.delete_if{|pipe| pipe.closed? }
    PIPE_MUTEX.synchronize do
      sout, sin = IO.pipe
      OPEN_PIPE_IN << sin

      [sout, sin]
    end
  end
  
  def self.release_pipes(*pipes)
    PIPE_MUTEX.synchronize do
      pipes.flatten.each do |pipe|
        pipe.close unless pipe.closed?
      end
    end
  end

  def self.purge_pipes(*save)
    PIPE_MUTEX.synchronize do
      OPEN_PIPE_IN.each do |pipe|
        next if save.include? pipe
        pipe.close unless pipe.closed?
      end
    end
  end

  def self.open_pipe(do_fork = false, close = true)
    raise "No block given" unless block_given?

    sout, sin = Misc.pipe

    if do_fork
      parent_pid = Process.pid
      pid = Process.fork {
        purge_pipes(sin)
        sout.close
        begin
          yield sin
          sin.close if close and not sin.closed? 
        rescue
          Log.exception $!
          Process.kill :INT, parent_pid
          Kernel.exit! -1
        end
        Kernel.exit! 0
      }
      sin.close 
      ConcurrentStream.setup sout, :pids => [pid]
    else
      thread = Thread.new(Thread.current) do |parent|
        begin
          yield sin
          sin.close if close and not sin.closed?
        rescue Aborted
          Log.medium "Aborted open_pipe: #{$!.message}"
        rescue Exception
          Log.medium "Exception in open_pipe: #{$!.message}"
          parent.raise $!
          raise $!
        end
      end
      ConcurrentStream.setup sout, :threads => [thread]
    end
    sout
  end

  def self.tee_stream_thread(stream)
    stream_out1, stream_in1 = Misc.pipe
    stream_out2, stream_in2 = Misc.pipe

    if ConcurrentStream === stream 
      stream.annotate stream_out1
    end

    splitter_thread = Thread.new(Thread.current) do |parent|
      begin
        skip1 = skip2 = false
        while block = stream.read(2048)
          begin 
            stream_in1.write block; 
          rescue IOError
            Log.medium("Tee stream 1 #{Misc.fingerprint stream} IOError: #{$!.message}");
            skip1 = true
          end unless skip1 

          begin 
            stream_in2.write block
          rescue IOError
            Log.medium("Tee stream 2 #{Misc.fingerprint stream} IOError: #{$!.message}");
            skip2 = true
          end unless skip2 
        end
        stream_in1.close unless stream_in1.closed?
        stream.join if stream.respond_to? :join
        stream_in2.close unless stream_in2.closed?
      rescue Aborted, Interrupt
        stream_out1.abort if stream_out1.respond_to? :abort
        stream.abort if stream.respond_to? :abort
        stream_out2.abort if stream_out2.respond_to? :abort
        Log.medium "Tee aborting #{Misc.fingerprint stream}"
        raise $!
      rescue Exception
        stream_out1.abort if stream_out1.respond_to? :abort
        stream.abort if stream.respond_to? :abort
        stream_out2.abort if stream_out2.respond_to? :abort
        Log.medium "Tee exception #{Misc.fingerprint stream}"
        raise $!
      end
    end

    ConcurrentStream.setup stream_out1, :threads => splitter_thread
    ConcurrentStream.setup stream_out2, :threads => splitter_thread

    stream_out1.callback = stream.callback if stream.respond_to? :callback
    stream_out1.abort_callback = stream.abort_callback if stream.respond_to? :abort_callback

    stream_out2.callback = stream.callback if stream.respond_to? :callback
    stream_out2.abort_callback = stream.abort_callback if stream.respond_to? :abort_callback

    [stream_out1, stream_out2]
  end

  class << self
    alias tee_stream tee_stream_thread 
  end

  def self.read_full_stream(io)
    str = ""
    begin
      while block = io.read(2048)
        str << block
      end
      io.join if io.respond_to? :join
    rescue
      io.abort if io.respond_to? :abort
    end
    str
  end

  def self.consume_stream(io, in_thread = false, into = nil)
    return if Path === io
    return unless io.respond_to? :read 
    if io.respond_to? :closed? and io.closed?
      io.join if io.respond_to? :join
      return
    end

    if in_thread
      Thread.new do
        consume_stream(io, false)
      end
    else
      Log.medium "Consuming stream #{Misc.fingerprint io}"
      begin
        into.sync == true if IO === into
        while not io.closed? and block = io.read(2048)
          into << block if into
        end
        io.join if io.respond_to? :join
        io.close unless io.closed?
      rescue Aborted
        Log.medium "Consume stream aborted #{Misc.fingerprint io}"
        io.abort if io.respond_to? :abort
        io.close unless io.closed?
      rescue Exception
        Log.medium "Exception consuming stream: #{Misc.fingerprint io}: #{$!.message}"
        io.abort if io.respond_to? :abort
        io.close unless io.closed?
        raise $!
      end
    end
  end

  def self.read_stream(stream, size)
    str = nil
    Thread.pass while IO.select([stream],nil,nil,1).nil?
    while not str = stream.read(size)
      IO.select([stream],nil,nil,1) 
      Thread.pass
      raise ClosedStream if stream.eof?
    end

    while str.length < size
      raise ClosedStream if stream.eof?
      IO.select([stream],nil,nil,1)
      if new = stream.read(size-str.length)
        str << new
      end
    end
    str
  end

  def self.sensiblewrite(path, content = nil, options = {}, &block)
    force = Misc.process_options options, :force
    return if Open.exists? path and not force
    lock_options = Misc.pull_keys options.dup, :lock
    lock_options = lock_options[:lock] if Hash === lock_options[:lock]
    tmp_path = Persist.persistence_path(path, {:dir => Misc.sensiblewrite_dir})
    tmp_path_lock = Persist.persistence_path(path, {:dir => Misc.sensiblewrite_lock_dir})
    Misc.lock tmp_path_lock, lock_options do
      return if Open.exists? path and not force
      FileUtils.mkdir_p File.dirname(tmp_path) unless File.directory? File.dirname(tmp_path)
      FileUtils.rm_f tmp_path if File.exists? tmp_path
      begin
        case
        when block_given?
          File.open(tmp_path, 'wb', &block)
        when String === content
          File.open(tmp_path, 'wb') do |f| f.write content end
        when (IO === content or StringIO === content or File === content)

          Open.write(tmp_path) do |f|
            f.sync = true
            while block = content.read(2048)
              f.write block
            end
          end
        else
          File.open(tmp_path, 'wb') do |f|  end
        end

        begin
          Open.mv tmp_path, path, lock_options
        rescue
          raise $! unless File.exists? path
        end
        content.join if content.respond_to? :join
      rescue Aborted
        Log.medium "Aborted sensiblewrite -- #{ Log.reset << Log.color(:blue, path) }"
        content.abort if content.respond_to? :abort
        Open.rm path if File.exists? path
      rescue Exception
        Log.medium "Exception in sensiblewrite: #{$!.message} -- #{ Log.color :blue, path }"
        content.abort if content.respond_to? :abort
        Open.rm path if File.exists? path
        raise $!
      ensure
        FileUtils.rm_f tmp_path if File.exists? tmp_path
      end
    end
  end

  def self.process_stream(s)
    begin
      yield s
      s.join if s.respond_to? :join
    rescue
      s.abort if s.respond_to? :abort
      raise $!
    end
  end

  def self.sort_stream(stream, header_hash = "#", cmd_args = " -u ")
    Misc.open_pipe do |sin|
      begin
        if defined? Step and Step === stream
          step = stream
          stream = stream.get_stream || stream.path.open
        end

        line = stream.gets
        while line =~ /^#{header_hash}/ do
          sin.puts line
          line = stream.gets
        end

        line_stream = Misc.open_pipe do |line_stream_in|
          begin
            while line
              line_stream_in.puts line
              line = stream.gets
            end
            stream.join if stream.respond_to? :join
          rescue
            stream.abort if stream.respond_to? :abort
            raise $!
          end
        end

        sorted = CMD.cmd("sort #{cmd_args || ""}", :in => line_stream, :pipe => true)

        while block = sorted.read(2048)
          sin.write block
        end
      rescue
        if defined? step and step
          step.abort
        end
      end
    end
  end

  def self.collapse_stream(s, line = nil, sep = "\t", header = nil)
    sep ||= "\t"
    Misc.open_pipe do |sin|
      sin.puts header if header
      process_stream(s) do |s|
        line ||= s.gets

        current_parts = []
        while line 
          key, *parts = line.strip.split(sep, -1)
          current_key ||= key
          case
          when key.nil?
          when current_key == key
            parts.each_with_index do |part,i|
              if current_parts[i].nil?
                current_parts[i] = part
              else
                current_parts[i] = current_parts[i] << "|" << part
              end
            end
          when current_key != key
            sin.puts [current_key, current_parts].flatten * sep
            current_key = key
            current_parts = parts
          end
          line = s.gets
        end

        sin.puts [current_key, current_parts].flatten * sep unless current_key.nil?
      end
    end
  end

  def self._paste_streams(streams, output, lines = nil, sep = "\t", header = nil)
    output.puts header if header
    streams = streams.collect do |stream|
      if defined? Step and Step === stream
        stream.get_stream || stream.join.path.open
      else
        stream
      end
    end

    begin
      done_streams = []
      lines ||= streams.collect{|s| s.gets }
      keys = []
      parts = []
      lines.each_with_index do |line,i|
        key, *p = line.strip.split(sep, -1) 
        keys[i] = key
        parts[i] = p
      end
      sizes = parts.collect{|p| p.length }
      last_min = nil
      while lines.compact.any?
        min = keys.compact.sort.first
        str = []
        keys.each_with_index do |key,i|
          case key
          when min
            str << [parts[i] * sep]
            line = lines[i] = streams[i].gets
            if line.nil?
              keys[i] = nil
              parts[i] = nil
            else
              k, *p = line.strip.split(sep, -1)
              keys[i] = k
              parts[i] = p
            end
          else
            str << [sep * (sizes[i]-1)] if sizes[i] > 0
          end
        end

        output.puts [min, str*sep] * sep
      end
      streams.each do |stream|
        stream.join if stream.respond_to? :join
      end
    rescue 
      Log.exception $!
      streams.each do |stream|
        stream.abort if stream.respond_to? :abort
      end
      raise $!
    end
  end

  def self.paste_streams(streams, lines = nil, sep = "\t", header = nil)
    sep ||= "\t"
    num_streams = streams.length
    Misc.open_pipe do |sin|
      self._paste_streams(streams, sin, lines, sep, header)
    end
  end

  def self.dup_stream(stream)
    stream_dup = stream.dup
    if stream.respond_to? :annotate
      stream.annotate stream_dup
      stream.clear
    end
    tee1, tee2 = Misc.tee_stream stream_dup
    stream.reopen(tee1)
    tee2
  end

  def self.save_stream(file, stream)
    out, save = Misc.tee_stream stream

    Thread.new(Thread.current) do |parent|
      begin
        Misc.sensiblewrite(file, save)
      rescue Exception
        save.abort if save.respond_to? :abort
        stream.abort if stream.respond_to? :abort
        stream.join
        Log.medium "Exception in save_stream: #{$!.message}"
        raise $!
      end
    end

    out
  end

end
