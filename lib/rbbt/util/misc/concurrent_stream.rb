module ConcurrentStream
  attr_accessor :threads, :pids, :callback, :abort_callback, :filename, :joined, :aborted, :autojoin

  def self.setup(stream, options = {}, &block)
    threads, pids, callback, filename, autojoin = Misc.process_options options, :threads, :pids, :callback, :filename, :autojoin
    stream.extend ConcurrentStream unless ConcurrentStream === stream

    stream.threads ||= []
    stream.pids ||= []
    stream.threads.concat(Array === threads ? threads : [threads]) unless threads.nil? 
    stream.pids.concat(Array === pids ? pids : [pids]) unless pids.nil? or pids.empty?
    stream.autojoin = autojoin

    callback = block if block_given?
    if stream.callback and callback
      old_callback = stream.callback
      stream.callback = Proc.new do
        old_callback.call
        callback.call
      end
    else
      stream.callback = callback
    end

    stream.filename = filename unless filename.nil?

    stream
  end

  def annotate(stream)
    ConcurrentStream.setup stream
    stream.threads = threads
    stream.pids = pids
    stream.callback = callback
    stream.abort_callback = abort_callback
    stream.filename = filename
    stream.autojoin = autojoin
    stream.joined = joined
  end

  def clear
    threads, pids, callback, abort_callback = nil
  end

  def joined?
    @joined
  end

  def aborted?
    @aborted
  end

  def join_threads
    if @threads and @threads.any?
      @threads.each do |t| 
        t.join unless t == Thread.current
      end
      @threads = []
    end
  end

  def join_pids
    if @pids and @pids.any?
      @pids.each do |pid| 
        begin
          Process.waitpid(pid, Process::WUNTRACED)
          raise ProcessFailed.new "Error joining process #{pid} in #{self.inspect}" unless $?.success?
        rescue Errno::ECHILD
        end
      end 
      @pids = []
    end
  end

  def join_callback
    if @callback and not joined?
      @callback.call
      @callback = nil
    end
  end

  def join
    join_threads
    join_pids

    join_callback

    @joined = true
    close unless closed?
  end

  def abort_threads
    Log.medium "Aborting threads (#{Thread.current.inspect}) #{@threads.collect{|t| t.inspect } * ", "}"

    @threads.each do |t| 
      @aborted = false if t == Thread.current
      next if t == Thread.current
      Log.medium "Aborting thread #{t.inspect}"
      t.raise Aborted.new 
    end if @threads

    sleeped = false
    @threads.each do |t|
      next if t == Thread.current
      if t.alive? 
        sleep 1 unless sleeped
        sleeped = true
        Log.medium "Kill thread #{t.inspect}"
        t.kill
      end
      begin
        Log.medium "Join thread #{t.inspect}"
        t.join unless t == Thread.current
      rescue Aborted
      rescue Exception
        Log.exception $!
      end
    end
    Log.medium "Aborted threads (#{Thread.current.inspect}) #{@threads.collect{|t| t.inspect } * ", "}"
  end

  def abort_pids
    @pids.each{|pid| Process.kill :INT, pid } if @pids
    @pids = []
  end

  def abort
    return if @aborted
    Log.medium "Aborting stream #{Misc.fingerprint self} -- #{@abort_callback} [#{@aborted}]"
    @aborted = true 
    begin
      @callback = nil
      @abort_callback.call if @abort_callback
      @abort_callback = nil
      close unless closed?

      abort_threads
      abort_pids
    end
    Log.medium "Aborted stream #{Misc.fingerprint self} -- #{@abort_callback} [#{@aborted}]"
  end

  def super(*args)
    if autojoin
      begin
        super(*args)
      rescue
        Log.exception $!
        self.abort
        self.join 
        raise $!
      ensure
        self.join if self.closed? or self.eof? 
      end
    else
      super(*args)
    end
  end

end
