require 'rbbt/util/concurrency/processes/worker'
require 'rbbt/util/concurrency/processes/socket'

class RbbtProcessQueue
  #{{{ RbbtProcessQueue

  attr_accessor :num_processes, :processes, :queue, :process_monitor, :cleanup, :join
  def initialize(num_processes, cleanup = nil, join = nil)
    @num_processes = num_processes
    @processes = []
    @cleanup = cleanup
    @join = join
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
            raise p if Exception === p
            raise p.first if Array === p and Exception === p.first
            @callback.call p
          end
        rescue Aborted
          parent.raise $!
        rescue ClosedStream
        rescue Exception
          Log.warn "Callback thread exception"
          parent.raise $!
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
      @processes << RbbtProcessQueueWorker.new(@queue, @callback_queue, @cleanup, &block)
    end
    @queue.close_read

    @process_monitor = Thread.new(Thread.current) do |parent|
      begin
        while @processes.any?
          @processes[0].join 
          @processes.shift
        end
      rescue Aborted
        @processes.each{|p| p.abort }
        @processes.each{|p| p.join }
        Log.warn "Process monitor aborted"
      rescue Exception
        Log.warn "Process monitor exception: #{$!.message}"
        @processes.each{|p| p.abort }
        @callback_thread.raise $! if @callback_thread
        parent.raise $!
      end
    end
  end

  def close_callback
    begin
      @callback_queue.push ClosedStream.new if @callback_thread.alive?
    rescue
      Log.warn "Error closing callback: #{$!.message}"
    end
    @callback_thread.join  if @callback_thread.alive?
  end

  def join
    @processes.length.times do 
      @queue.push ClosedStream.new
    end if @process_monitor.alive?

    begin
      @process_monitor.join
      close_callback if @callback
    rescue Exception
      Log.exception $!
      raise $!
    ensure
      @queue.swrite.close unless @queue.swrite.closed?
    end

    @join.call if @join
  end

  def clean
    if @process_monitor.alive? or @callback_thread.alive?
      self.abort
    else
      self.join
    end
  end

  def abort
    begin
      @process_monitor.raise(Aborted.new); @process_monitor.join if @process_monitor and @process_monitor.alive?
      @callback_thread.raise(Aborted.new); @callback_thread.join if @callback_thread and @callback_thread.alive?
    ensure
      join
    end
  end

  def process(*e)
    @queue.push e
  end

  def self.each(list, num = 3, &block)
    q = RbbtProcessQueue.new num
    q.init(&block)
    list.each do |elem| q.process elem end
    q.join
  end
end
