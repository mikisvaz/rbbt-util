require 'rbbt/util/concurrency/processes/worker'
require 'rbbt/util/concurrency/processes/socket'

class RbbtProcessQueue
  #{{{ RbbtProcessQueue

  attr_accessor :num_processes, :processes, :queue, :process_monitor, :cleanup
  def initialize(num_processes, cleanup = nil)
    @num_processes = num_processes
    @processes = []
    @cleanup = cleanup
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
        rescue ClosedStream
        rescue Exception
          Log.exception $!
          sleep 1
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
      rescue Exception
        @processes.each do |p|
          begin
            Process.kill :INT, p
          rescue
          end
        end
        Log.exception $!
        parent.raise $!
      end
    end
  end

  def close_callback
    @callback_queue.push ClosedStream.new if  @callback_thread.alive?
    @callback_queue.swrite.close 
    @callback_thread.join 
  end

  def join
    @processes.length.times do 
      @queue.push ClosedStream.new
    end
    begin
      @process_monitor.join
      close_callback if @callback
    rescue Exception
      Log.exception $!
    ensure
      @queue.swrite.close
    end
  end

  def clean
    @processes.each{|p| p.abort }
    @callback_thread.raise Aborted if @callback_thread and @callback_thread.alive?
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
