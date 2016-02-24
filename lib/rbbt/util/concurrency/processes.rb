require 'rbbt/util/concurrency/processes/worker'
require 'rbbt/util/concurrency/processes/socket'

class RbbtProcessQueue

  attr_accessor :num_processes, :processes, :queue, :process_monitor, :cleanup, :join, :reswpan
  def initialize(num_processes, cleanup = nil, join = nil, reswpan = nil)
    @num_processes = num_processes
    @processes = []
    @cleanup = cleanup
    @join = join
    @respawn = reswpan
    @queue = RbbtProcessSocket.new
  end

  attr_accessor :callback, :callback_queue, :callback_thread
  def callback(&block)
    if block_given?
    
      @callback = block

      @callback_queue = RbbtProcessSocket.new

      @callback_thread = Thread.new(Thread.current) do |parent|
        begin
          loop do
            p = @callback_queue.pop

            if Exception === p or (Array === p and Exception === p.first)
              e = Array === p ? p.first : p
              Log.warn "Callback recieved exception from worker: #{e.message}" unless Aborted === e or ClosedStream === e
              raise e 
            end

            if @callback.arity == 0
              @callback.call
            else
              @callback.call p
            end
          end
        rescue Aborted
          Log.warn "Callback thread aborted"
          @process_monitor.raise $!
          raise $!
        rescue ClosedStream
        rescue Exception
          Log.warn "Exception captured in callback: #{$!.message}"
          @process_monitor.raise $!
          raise $!
        ensure

          @callback_queue.sread.close unless @callback_queue.sread.closed?
        end
      end
    else
      @callback, @callback_queue, @callback_thread = nil, nil, nil
    end
  end

  def init(&block)
    num_processes.times do |i|
      @processes << RbbtProcessQueueWorker.new(@queue, @callback_queue, @cleanup, @respawn, &block)
    end
    @queue.close_read

    @process_monitor = Thread.new(Thread.current) do |parent|
      begin
        while @processes.any?
          @processes[0].join 
          @processes.shift
        end
      rescue Aborted
        Log.warn "Aborting process monitor"
        @processes.each{|p| p.abort }
        @processes.each{|p| 
          begin
            p.join 
          rescue ProcessFailed
          end
        }
      rescue Exception
        Log.warn "Process monitor exception: #{$!.message}"
        @processes.each{|p| p.abort }
        @callback_thread.raise $! if @callback_thread and @callback_thread.alive?
        raise $!
      end
    end
  end

  def close_callback
    begin
      @callback_queue.push ClosedStream.new if @callback_thread.alive?
    rescue Exception
      Log.warn "Error closing callback: #{$!.message}"
    end
    @callback_thread.join  #if @callback_thread.alive?
  end

  def join
    begin
      @processes.length.times do 
        @queue.push ClosedStream.new
      end if @process_monitor.alive?
    rescue Exception
    end

    begin
      @process_monitor.join
      close_callback if @callback
    rescue Aborted
      Log.error "Aborted joining queue"
      raise $!
    rescue Exception
      Log.error "Exception joining queue: #{$!.message}"
      raise $!
    ensure
      @queue.swrite.close unless @queue.swrite.closed?
    end

    @join.call if @join
  end

  def clean
    if (@process_monitor and @process_monitor.alive?) or (@callback_thread and @callback_thread.alive?)
      self.abort 
      self.join
    end

    @queue.clean if @queue
    @callback_queue.clean if @callback_queue
  end

  def abort
    begin
      (@process_monitor.raise(Aborted.new); @process_monitor.join) if @process_monitor and @process_monitor.alive?
      (@callback_thread.raise(Aborted.new); @callback_thread.join) if @callback_thread and @callback_thread.alive?
    ensure
      begin
        join
      rescue ProcessFailed
      end
    end
  end

  def process(*e)
    begin
      @queue.push e
    rescue Errno::EPIPE
      raise Aborted
    end
  end

  def self.each(list, num = 3, &block)
    q = RbbtProcessQueue.new num
    q.init(&block)
    list.each do |elem| q.process elem end
    q.join
  end
end
