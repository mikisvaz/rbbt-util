module AbortedStream
  attr_accessor :exception
  def self.setup(obj, exception = nil)
    obj.extend AbortedStream
    obj.exception = exception
  end
end

module ConcurrentStream
  attr_accessor :threads, :pids, :callback, :abort_callback, :filename, :joined, :aborted, :autojoin, :lockfile, :no_fail, :pair, :thread

  def self.setup(stream, options = {}, &block)
    
    threads, pids, callback, abort_callback, filename, autojoin, lockfile, no_fail, pair = Misc.process_options options, :threads, :pids, :callback, :abort_callback, :filename, :autojoin, :lockfile, :no_fail, :pair
    stream.extend ConcurrentStream unless ConcurrentStream === stream

    stream.threads ||= []
    stream.pids ||= []
    stream.threads.concat(Array === threads ? threads : [threads]) unless threads.nil? 
    stream.pids.concat(Array === pids ? pids : [pids]) unless pids.nil? or pids.empty?
    stream.autojoin = autojoin unless autojoin.nil?
    stream.no_fail = no_fail unless no_fail.nil?

    stream.pair = pair unless pair.nil?

    callback = block if block_given?
    if callback
      if stream.callback
        old_callback = stream.callback
        stream.callback = Proc.new do
          old_callback.call
          callback.call
        end
      else
        stream.callback = callback 
      end
    end

    if abort_callback
      if stream.abort_callback
        old_abort_callback = stream.abort_callback
        stream.abort_callback = Proc.new do
          old_abort_callback.call
          abort_callback.call
        end
      else
        stream.abort_callback = abort_callback 
      end
    end

    stream.filename = filename unless filename.nil?

    stream.lockfile = lockfile unless lockfile.nil?

    stream.aborted = false

    stream
  end

  def annotate(stream)
    ConcurrentStream.setup(stream, :threads => threads, :pids => pids, :callback => callback, :abort_callback => abort_callback, :filename => filename, :autojoin => autojoin, :lockfile => lockfile)
    stream
  end

  def clear
    threads, pids, callback, abort_callback, joined = nil
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
        next if t == Thread.current
        begin
          t.join #unless FalseClass === t.status 
        rescue Exception
          if no_fail
            Log.low "Not failing on exception joining thread in ConcurrenStream: #{filename}"
          else
            Log.low "Exception joining thread in ConcurrenStream: #{filename}"
            stream_raise_exception $! 
          end
        end
      end
    end
    @threads = []
  end

  def join_pids
    if @pids and @pids.any?
      @pids.each do |pid| 
        begin
          Process.waitpid(pid, Process::WUNTRACED)
          stream_raise_exception ProcessFailed.new "Error joining process #{pid} in #{self.filename || self.inspect}" unless $?.success? or no_fail
        rescue Errno::ECHILD
        end
      end 
      @pids = []
    end
  end

  def join_callback
    if @callback and not joined?
      begin
        @callback.call
      ensure
        @callback = nil
      end
    end
  end

  def join
    begin
      join_threads
      join_pids
      join_callback
      close unless closed?
    ensure
      @joined = true
      lockfile.unlock if lockfile and lockfile.locked?
    end
  end

  def abort_threads(exception = nil)
    return unless @threads and @threads.any?
    name = Thread.current.inspect
    name = filename if filename
    Log.low "Aborting threads (#{name}) #{@threads.collect{|t| t.inspect } * ", "}"

    @threads.each do |t| 
      next if t == Thread.current
      Log.debug "Aborting thread (#{name}) #{t.inspect} with exception: #{exception}"
      t.raise((exception.nil? ? Aborted.new : exception))
    end 

    @threads.each do |t|
      next if t == Thread.current
      if t.alive? 
        sleep 1
        Log.low "Kill thread (#{name}) #{t.inspect}"
        t.kill
      end
      begin
        t.join unless t == Thread.current
      rescue Aborted
      rescue Exception
        Log.debug "Thread (#{name}) exception: #{$!.message}"
      end
    end
  end

  def abort_pids
    @pids.each do |pid|
      begin 
        Log.low "Killing PID #{pid} in ConcurrentStream #{filename}"
        Process.kill :INT, pid 
      rescue Errno::ESRCH
      end
    end if @pids
    @pids = []
  end

  def abort(exception = nil)
    if @aborted
      Log.medium "Already aborted stream #{Misc.fingerprint self} [#{@aborted}]"
      return
    else
      Log.medium "Aborting stream #{Misc.fingerprint self} [#{@aborted}]"
    end
    AbortedStream.setup(self, exception)
    @aborted = true 
    begin
      close unless closed?

      @abort_callback.call exception if @abort_callback

      abort_threads(exception)
      abort_pids

      @callback = nil
      @abort_callback = nil

      @pair.abort exception if @pair
    ensure
      if lockfile and lockfile.locked?
        lockfile.unlock 
      end
    end
  end

  def close(*args)
    if autojoin
      begin
        super(*args)
      rescue
        Log.exception $!
        self.abort
        self.join 
        stream_raise_exception $!
      ensure
        self.join if self.closed? or self.eof? 
      end
    else
      super(*args)
    end
  end

  def add_callback(&block)
    old_callback = callback
    @callback = Proc.new do 
      old_callback.call if old_callback
      block.call
    end
  end

  def stream_raise_exception(exception)
    threads.each do |thread|
      thread.raise exception
    end

    self.abort
  end

end
