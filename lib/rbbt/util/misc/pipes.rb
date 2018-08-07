require 'rbbt'

module Misc
  class << self
    attr_accessor :sensiblewrite_lock_dir
    
    def sensiblewrite_lock_dir
      @sensiblewrite_lock_dir ||= Rbbt.tmp.sensiblewrite_locks.find
    end
  end

  class << self
    attr_accessor :sensiblewrite_dir
    def sensiblewrite_dir
      @sensiblewrite_dir = Rbbt.tmp.sensiblewrite.find
    end
  end

  BLOCK_SIZE=1024 * 8

  PIPE_MUTEX = Mutex.new

  OPEN_PIPE_IN = []
  def self.pipe
    OPEN_PIPE_IN.delete_if{|pipe| pipe.closed? }
    res = PIPE_MUTEX.synchronize do
      sout, sin = IO.pipe
      OPEN_PIPE_IN << sin

      [sout, sin]
    end
    Log.debug{"Creating pipe #{[res.last.inspect,res.first.inspect] * " => "}"}
    res
  end

  def self.with_fifo(path = nil, &block)
    begin
      erase = path.nil?
      path = TmpFile.tmp_file if path.nil?
      File.mkfifo path
      yield path
    ensure
      FileUtils.rm path if erase
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

        rescue Exception
          Log.exception $!
          Process.kill :INT, parent_pid
          Kernel.exit! -1
        end
        Kernel.exit! 0
      }
      sin.close

      ConcurrentStream.setup sout, :pids => [pid]
    else


      ConcurrentStream.setup sin, :pair => sout

      thread = Thread.new(Thread.current) do |parent|
        begin
          
          yield sin

          sin.close if close and not sin.closed? and not sin.aborted?

        rescue Aborted
          Log.medium "Aborted open_pipe: #{$!.message}"
        rescue Exception
          Log.medium "Exception in open_pipe: #{$!.message}"
          Log.exception $!
          parent.raise $!
          raise $!
        end
      end


      sin.threads = [thread]
      ConcurrentStream.setup sout, :threads => [thread], :pair => sin
    end

    sout
  end

  #def self.tee_stream_thread(stream)
  #  stream_out1, stream_in1 = Misc.pipe
  #  stream_out2, stream_in2 = Misc.pipe

  #  splitter_thread = Thread.new(Thread.current) do |parent|
  #    begin

  #      skip1 = skip2 = false
  #      while block = stream.read(1024)

  #        begin 
  #          stream_in1.write block; 
  #        rescue IOError
  #          Log.medium("Tee stream 1 #{Misc.fingerprint stream} IOError: #{$!.message}");
  #          skip1 = true
  #        end unless skip1 

  #        begin 
  #          stream_in2.write block
  #        rescue IOError
  #          Log.medium("Tee stream 2 #{Misc.fingerprint stream} IOError: #{$!.message}");
  #          skip2 = true
  #        end unless skip2 

  #      end

  #      stream_in1.close unless stream_in1.closed?
  #      stream.join if stream.respond_to? :join
  #      stream_in2.close unless stream_in2.closed?
  #    rescue Aborted, Interrupt
  #      stream_out1.abort if stream_out1.respond_to? :abort
  #      stream.abort if stream.respond_to? :abort
  #      stream_out2.abort if stream_out2.respond_to? :abort
  #      Log.medium "Tee aborting #{Misc.fingerprint stream}"
  #      raise $!
  #    rescue Exception
  #      stream_out1.abort if stream_out1.respond_to? :abort
  #      stream.abort if stream.respond_to? :abort
  #      stream_out2.abort if stream_out2.respond_to? :abort
  #      Log.medium "Tee exception #{Misc.fingerprint stream}"
  #      raise $!
  #    end
  #  end

  #  ConcurrentStream.setup stream_out1, :threads => splitter_thread
  #  ConcurrentStream.setup stream_out2, :threads => splitter_thread

  #  [stream_out1, stream_out2]
  #end

  def self.tee_stream_thread_multiple(stream, num = 2)
    in_pipes = []
    out_pipes = []
    num.times do 
      sout, sin = Misc.pipe
      in_pipes << sin
      out_pipes << sout
    end

    filename = stream.filename if stream.respond_to? :filename

    splitter_thread = Thread.new(Thread.current) do |parent|
      begin

        skip = [false] * num
        begin
          while block = stream.readpartial(BLOCK_SIZE)

            in_pipes.each_with_index do |sin,i|
              begin 
                sin.write block
              rescue IOError
                Log.error("Tee stream #{i} #{Misc.fingerprint stream} IOError: #{$!.message}");
                skip[i] = true
              end unless skip[i] 
            end
          end
        rescue IOError
        end

        stream.close unless stream.closed?
        stream.join if stream.respond_to? :join
        in_pipes.first.close
        #Log.medium "Tee done #{Misc.fingerprint stream}"
      rescue Aborted, Interrupt
        stream.abort if stream.respond_to? :abort
        out_pipes.each do |sout|
          sout.abort if sout.respond_to? :abort
        end
        Log.medium "Tee aborting #{Misc.fingerprint stream}"
        raise $!
      rescue Exception
        stream.abort($!) if stream.respond_to? :abort
        out_pipes.each do |sout|
          sout.abort if sout.respond_to? :abort
        end
        Log.medium "Tee exception #{Misc.fingerprint stream}"
        raise $!
      end
    end

    out_pipes.each do |sout|
      ConcurrentStream.setup sout, :threads => splitter_thread, :filename => filename, :_pair => stream
    end

    main_pipe = out_pipes.first
    main_pipe.autojoin = true

    main_pipe.callback = Proc.new do 
      stream.join if stream.respond_to? :join
      in_pipes[1..-1].each do |sin|
        sin.close unless sin.closed?
      end
    end

    out_pipes
  end

  def self.tee_stream_thread(stream)
    tee_stream_thread_multiple(stream, 2)
  end

  def self.dup_stream_multiple(stream, num = 1)
    stream_dup = stream.dup
    if stream.respond_to? :annotate
      stream.annotate stream_dup
      stream.clear
    end
    tee1, *rest = Misc.tee_stream stream_dup, num + 1
    stream.reopen(tee1)
    rest
  end

  def self.dup_stream(stream)
    dup_stream_multiple(stream, 1).first
  end

  class << self
    alias tee_stream tee_stream_thread_multiple
  end

  def self.read_full_stream(io)
    str = ""
    begin
      while block = io.read(BLOCK_SIZE)
        str << block
      end
      io.join if io.respond_to? :join
    rescue
      io.abort if io.respond_to? :abort
    end
    str
  end

  def self.consume_stream(io, in_thread = false, into = nil, into_close = true, &block)
    return if Path === io
    return unless io.respond_to? :read 

    if io.respond_to? :closed? and io.closed?
      io.join if io.respond_to? :join
      return
    end

    if in_thread
      Thread.new(Thread.current) do |parent|
        begin
          consume_stream(io, false, into, into_close)
        rescue Exception
          parent.raise $!
        end
      end
    else
      if into
        Log.medium "Consuming stream #{Misc.fingerprint io} -> #{Misc.fingerprint into}"
      else
        Log.medium "Consuming stream #{Misc.fingerprint io}"
      end

      begin
        into = into.find if Path === into
        if String === into 
          dir = File.dirname(into)
          FileUtils.mkdir_p dir unless Open.exists?(dir)
          into_path, into = into, Open.open(into, :mode => 'w') 
        end
        into.sync = true if IO === into
        into_close = false unless into.respond_to? :close
        io.sync = true

        begin
          while c = io.readpartial(BLOCK_SIZE)
            into << c if into
          end
        rescue EOFError
        end

        io.join if io.respond_to? :join
        io.close unless io.closed?
        into.close if into and into_close and not into.closed?
        into.join if into and into_close and into.respond_to?(:joined?) and not into.joined?
        block.call if block_given?

        #Log.medium "Done consuming stream #{Misc.fingerprint io}"
      rescue Aborted
        Log.medium "Consume stream aborted #{Misc.fingerprint io}"
        io.abort if io.respond_to? :abort
        #io.close unless io.closed?
        FileUtils.rm into_path if into_path and File.exists? into_path
      rescue Exception
        Log.medium "Exception consuming stream: #{Misc.fingerprint io}: #{$!.message}"
        io.abort $! if io.respond_to? :abort
        FileUtils.rm into_path if into_path and File.exists? into_path
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

    if Open.exists? path and not force
      Misc.consume_stream content 
      return
    end

    lock_options = Misc.pull_keys options.dup, :lock
    lock_options = lock_options[:lock] if Hash === lock_options[:lock]
    tmp_path = Persist.persistence_path(path, {:dir => Misc.sensiblewrite_dir})
    tmp_path_lock = Persist.persistence_path(path, {:dir => Misc.sensiblewrite_lock_dir})

    tmp_path_lock = nil if FalseClass === options[:lock]

    Misc.lock tmp_path_lock, lock_options do

      if Open.exists? path and not force
        Log.warn "Path exists in sensiblewrite, not forcing update: #{ path }"
        Misc.consume_stream content 
      else
        FileUtils.mkdir_p File.dirname(tmp_path) unless File.directory? File.dirname(tmp_path)
        FileUtils.rm_f tmp_path if File.exist? tmp_path
        begin

          case
          when block_given?
            File.open(tmp_path, 'wb', &block)
          when String === content
            File.open(tmp_path, 'wb') do |f| f.write content end
          when (IO === content or StringIO === content or File === content)

            Open.write(tmp_path) do |f|
              f.sync = true
              while block = content.read(BLOCK_SIZE)
                f.write block
              end 
            end
          else
            File.open(tmp_path, 'wb') do |f|  end
          end

          begin
            Misc.insist do
              Open.mv tmp_path, path, lock_options
            end
          rescue Exception
            raise $! unless File.exist? path
          end

          FileUtils.touch path if File.exist? path
          content.join if content.respond_to? :join and not (content.respond_to?(:joined?) and content.joined?)

          if Lockfile === lock_options[:lock] and lock_options[:lock].locked?
            lock_options[:lock].unlock
          end
          Open.notify_write(path) 
        rescue Aborted
          Log.medium "Aborted sensiblewrite -- #{ Log.reset << Log.color(:blue, path) }"
          content.abort if content.respond_to? :abort
          Open.rm path if File.exist? path
        rescue Exception
          exception = (AbortedStream === content and content.exception) ? content.exception : $!
          Log.medium "Exception in sensiblewrite: [#{Process.pid}] #{exception.message} -- #{ Log.color :blue, path }"
          content.abort if content.respond_to? :abort
          Open.rm path if File.exist? path
          raise exception
        rescue
          Log.exception $!
          raise $!
        ensure
          FileUtils.rm_f tmp_path if File.exist? tmp_path
        end
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

  def self.sort_stream(stream, header_hash = "#", cmd_args = "-u")
    Misc.open_pipe do |sin|
      begin
        stream = TSV.get_stream stream

        line = stream.gets
        while line =~ /^#{header_hash}/ do
          sin.puts line
          line = stream.gets
        end

        line_stream = Misc.open_pipe do |line_stream_in|
          line_stream_in.puts line
          Misc.consume_stream(stream, false, line_stream_in)
        end

        sorted = CMD.cmd("env LC_ALL=C sort #{cmd_args || ""}", :in => line_stream, :pipe => true)

        Misc.consume_stream(sorted, false, sin)
      rescue
        if defined? step and step
          step.abort
        end
      end
    end
  end

  def self.collapse_stream(s, line = nil, sep = "\t", header = nil, &block)
    sep ||= "\t"
    Misc.open_pipe do |sin|
      sin.puts header if header
      process_stream(s) do |s|
        line ||= s.gets

        current_parts = []
        while line 
          key, *parts = line.chomp.split(sep, -1)
          case
          when key.nil?
          when current_parts.nil?
            current_parts = parts
            current_key = key
          when current_key == key
            parts.each_with_index do |part,i|
              if current_parts[i].nil?
                current_parts[i] = "|" << part
              else
                current_parts[i] = current_parts[i] << "|" << part
              end
            end

            (parts.length..current_parts.length-1).to_a.each do |pos|
              current_parts[pos] = current_parts[pos] << "|" << ""
            end
          when current_key.nil?
            current_key = key
            current_parts = parts
          when current_key != key
            if block_given?
              res = block.call(current_parts)
              sin.puts [current_key, res] * sep
            else
              sin.puts [current_key, current_parts].flatten * sep
            end 
            current_key = key
            current_parts = parts
          end
          line = s.gets
        end

        if block_given?
          res = block.call(current_parts)
          sin.puts [current_key, res] * sep
        else
          sin.puts [current_key, current_parts].flatten * sep
        end unless current_key.nil?
      end
    end
  end

  def self._paste_streams(streams, output, lines = nil, sep = "\t", header = nil, &block)
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
        if line.nil?
          keys[i] = nil
          parts[i] = []
        else
          key, *p = line.chomp.split(sep, -1) 
          keys[i] = key
          parts[i] = p
        end
      end
      sizes = parts.collect{|p| p.nil? ? 0 : p.length }
      last_min = nil
      while lines.compact.any?
        if block_given?
          min = keys.compact.sort(&block).first
        else
          min = keys.compact.sort.first
        end
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
              k, *p = line.chomp.split(sep, -1)
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

  def self.paste_streams(streams, lines = nil, sep = "\t", header = nil, &block)
    sep ||= "\t"
    num_streams = streams.length
    Misc.open_pipe do |sin|
      self._paste_streams(streams, sin, lines, sep, header, &block)
    end
  end

  def self.save_stream(file, stream)
    out, save = Misc.tee_stream stream
    out.filename = file
    save.filename = file

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

  def self.intercalate_streams(streams)
    Misc.open_pipe do |sin|
      continue = true
      while continue
        lines = streams.collect{|stream| stream.eof? ? nil : stream.gets }.compact
        lines.each do |line|
          sin.puts line
        end
        continue = false if lines.empty?
      end
      streams.each do |stream| 
        stream.join if stream.respond_to? :join
        stream.close if stream.respond_to? :close and not stream.closed?
      end
    end
  end

  def self.compare_lines(stream1, stream2, args, sort = false)
    if sort
      stream1 = Misc.sort_stream stream1
      stream2 = Misc.sort_stream stream2
      compare_lines(stream1, stream2, args, false)
    else
      erase = []

      if Path === stream1 or (String === stream1 and File.exist? stream1)
        file1 = stream1
      else
        file1 = TmpFile.tmp_file
        erase << file1
        Misc.consume_stream(TSV.get_stream(stream1), false, file1)
      end

      if Path === stream2 or (String === stream2 and File.exist? stream2)
        file2 = stream2
      else
        file2 = TmpFile.tmp_file
        erase << file2
        Misc.consume_stream(TSV.get_stream(stream2), false, file2)
      end

      CMD.cmd("env LC_ALL=C comm #{args} '#{file1}' '#{file2}'", :pipe => true, :post => Proc.new{ erase.each{|f| FileUtils.rm f } }) 
    end
  end

  def self.remove_lines(stream1, stream2, sort)
    self.compare_lines(stream1, stream2, '-2 -3', sort)
  end

  def self.select_lines(stream1, stream2, sort)
    self.compare_lines(stream1, stream2, '-1 -2', sort)
  end


  def self.add_stream_filename(io, filename)
    if ! io.respond_to? :filename
      class << io
        attr_accessor :filename
      end
      io.filename = filename
    end
  end

  def self.sort_mutation_stream_strict(stream, sep=":")
    CMD.cmd("grep '#{sep}' | sort -u | sed 's/^M:/MT:/' | env LC_ALL=C sort -V -k1,1 -k2,2n -k3,3n -t'#{sep}'", :in => stream, :pipe => true, :no_fail => true)
  end

  def self.sort_mutation_stream(stream, sep=":")
    CMD.cmd("grep '#{sep}' | sort -u | sed 's/^M:/MT:/' | env LC_ALL=C sort -k1,1 -k2,2n -k3,3n -t'#{sep}'", :in => stream, :pipe => true, :no_fail => true)
  end

  def self.swap_quoted_character(stream, charout="\n", charin=" ", quote='"')
    io = Misc.open_pipe do |sin|
      begin
        quoted = false
        prev = nil
        while c = stream.getc
          if c == quote and not prev == "\\"
            quoted = ! quoted
          end
          c = charin if c == charout and quoted
          sin << c
          prev = c
        end
      rescue
        stream.abort if stream.respond_to? :abort
        raise $!
      ensure
        stream.join if stream.respond_to? :join
      end
    end
  end

  def self.remove_quoted_new_line(stream, quote = '"')
    swap_quoted_character(stream, "\n", " ", quote)
  end

  def self.line_monitor_stream(stream, &block)
    monitor, out = tee_stream stream
    monitor_thread = Thread.new do
      begin
        while line = monitor.gets
          block.call line
        end
      rescue
        Log.exception $!
        monitor.raise $!
        monitor.close unless monitor.closed?
        monitor.join if monitor.respond_to?(:join) && ! monitor.aborted?
        out.raise $! if out.respond_to?(:raise)
      ensure
        monitor.close unless monitor.closed?
        monitor.join if monitor.respond_to?(:join) && ! monitor.aborted?
      end
    end

    stream.annotate out if stream.respond_to? :annotate
    ConcurrentStream.setup out, :threads => monitor_thread
  end

end
