require 'filelock'
require 'rbbt/util/concurrency/processes/worker'
require 'rbbt/util/concurrency/processes/socket'


class RbbtProcessQueue
  class ClosedQueue < Exception; end
  class Waiting < Exception; end

  #{{{ RbbtProcessQueue

  attr_accessor :num_processes, :processes, :queue, :lockfile, :process_monitor
  def initialize(num_processes, lockfile = TmpFile.tmp_file)
    @num_processes = num_processes
    @processes = []
    @lockfile = lockfile
    @queue = RbbtProcessSocket.new @lockfile
  end

  attr_accessor :callback, :callback_queue, :callback_thread
  def callback(&block)
    if block_given?
      @callback = block

      @callback_queue = RbbtProcessSocket.new @lockfile + '.callback'

      @callback_thread = Thread.new(Thread.current) do |parent|
        begin
          loop do
            p = @callback_queue.pop
            raise p if Exception === p
            @callback.call *p
          end
        rescue ClosedQueue
        rescue
          Log.debug $!
          parent.raise $!
          Thread.exit
        end
      end
    else
      @callback, @callback_queue, @callback_thread = nil, nil, nil
    end
  end

  def init(use_mutex = false, &block)
    num_processes.times do |i|
      @processes << RbbtProcessQueueWorker.new(@queue, @callback_queue, &block)
    end
    queue.sout.close
    @callback_queue.sin.close if @callback_queue

    @process_monitor = Thread.new(Thread.current) do |parent|
      begin
        while @processes.any? do
          pid = Process.wait -1, Process::WNOHANG
          if pid
            @processes.delete_if{|p| p.pid == pid}
            raise "Process #{pid} failed" unless $?.success?
          else
            Thread.pass
          end
        end
      rescue
        parent.raise $!
        Thread.exit
      end
    end
  end

  def close_callback
    @callback_thread.join if @callback_thread and @callback_thread.alive?
  end

  def join
    queue.sin.close
    begin
      @process_monitor.join
    ensure
      clean
      close_callback if @callback
    end
  end

  def clean
    @processes.each{|p| p.abort }.clear
    @queue.clean
    @callback_thread.raise Aborted if @callback_thread and @callback_thread.alive?
    @callback_queue.clean if @callback_queue
  end

  def process(e)
    @queue.push(Array === e ? e : [e])
  end
end
