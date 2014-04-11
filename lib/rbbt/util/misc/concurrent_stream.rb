module ConcurrentStream
  attr_accessor :threads, :pids, :callback, :abort_callback, :filename, :joined

  def joined?
    @joined
  end

  def join

    if @threads and @threads.any?
      @threads.each do |t| 
        t.join 
      end
      @threads = []
    end

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

    if @callback and not joined?
      @callback.call
      @callback = nil
    end

    @joined = true
  end

  def abort
    @threads.each{|t| t.raise Aborted.new } if @threads
    @threads.each{|t| t.join } if @threads
    @pids.each{|pid| Process.kill :INT, pid } if @pids
    @pids.each{|pid| Process.waitpid pid } if @pids
    @abort_callback.call if @abort_callback
    @abort_callback = nil
  end

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
end
