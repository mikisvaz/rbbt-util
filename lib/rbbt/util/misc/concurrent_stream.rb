module ConcurrentStream
  attr_accessor :threads, :pids, :callback, :abort_callback, :filename, :joined

  def self.setup(stream, options = {}, &block)
    threads, pids, callback, filename = Misc.process_options options, :threads, :pids, :callback, :filename
    stream.extend ConcurrentStream unless ConcurrentStream === stream

    stream.threads ||= []
    stream.pids ||= []
    stream.threads.concat(Array === threads ? threads : [threads]) unless threads.nil? 
    stream.pids.concat(Array === pids ? pids : [pids]) unless pids.nil? or pids.empty?

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
    stream.joined = joined
  end

  def clear
    threads, pids, callback, abort_callback = nil
  end

  def joined?
    @joined
  end

  def join_threads
    if @threads and @threads.any?
      @threads.each do |t| 
        begin
        ensure
          t.join unless t == Thread.current
        end
      end
      @threads = []
    end
  end

  def join_pids
    if @pids and @pids.any?
      @pids.each do |pid| 
        begin
          Process.waitpid(pid, Process::WUNTRACED)
          raise "Error joining process #{pid} in #{self.inspect}" unless $?.success?
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
    @threads.each{|t| t.raise Aborted.new unless t == Thread.current } if @threads
  end

  def abort_pids
    @pids.each{|pid| Process.kill :INT, pid } if @pids
  end

  def abort
    abort_threads
    abort_pids
    @abort_callback.call if @abort_callback
    @abort_callback = nil
    @callback = nil
  end

end
