require 'rbbt/util/concurrency/processes/worker'
require 'rbbt/util/concurrency/processes/socket'


class RbbtProcessQueue
  class Waiting < Exception; end

  #{{{ RbbtProcessQueue

  attr_accessor :num_processes, :processes, :queue, :process_monitor
  def initialize(num_processes)
    @num_processes = num_processes
    @processes = []
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
            @callback.call p
          end
        rescue RbbtProcessQueue::RbbtProcessSocket::ClosedSocket
        rescue Exception
          Log.debug $!
          parent.raise $!
          Thread.exit
        end
      end
    else
      @callback, @callback_queue, @callback_thread = nil, nil, nil
    end
  end

  def init(&block)
    num_processes.times do |i|
      @processes << RbbtProcessQueueWorker.new(@queue, @callback_queue, &block)
    end
    @queue.sread.close
    @callback_queue.swrite.close if @callback_queue

    @process_monitor = Thread.new(Thread.current) do |parent|
      begin
        while @processes.any? do
          pid = Process.wait -1, Process::WNOHANG
          if pid
            @processes.delete_if{|p| p.pid == pid}
            raise "Process #{pid} failed" unless $?.success?
          else
            sleep 1
          end
        end
      rescue
        parent.raise $!
      ensure
        Thread.exit
      end
    end
  end

  def close_callback
    @callback_thread.join if @callback_thread and @callback_thread.alive?
  end

  def join
    @queue.push RbbtProcessQueue::RbbtProcessSocket::ClosedSocket.new
    @queue.swrite.close
    begin
      @process_monitor.join
    ensure
      close_callback if @callback
    end
  end

  def clean
    @processes.each{|p| p.abort }.clear
    @callback_thread.raise Aborted if @callback_thread and @callback_thread.alive?
  end

  def process(e)
    @queue.push e
  end

  def self.each(list, num = 3, &block)
    q = RbbtProcessQueue.new num
    q.init(&block)
    list.each do |elem| q.process elem end
    q.join
  end
end
