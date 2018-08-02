require 'rbbt/util/concurrency/processes/worker'
require 'rbbt/util/concurrency/processes/socket'

class RbbtProcessQueue

  attr_accessor :num_processes, :processes, :queue, :process_monitor, :cleanup, :join, :reswpan, :offset
  def initialize(num_processes, cleanup = nil, join = nil, reswpan = nil, offset = false)
    @num_processes = num_processes
    @processes = []
    @cleanup = cleanup
    @join = join
    @respawn = reswpan
    @offset = offset
    @queue = RbbtProcessSocket.new
    @process_mutex = Mutex.new
  end


  ABORT_SIGNAL = :INT
  CLOSE_SIGNAL = :PIPE

  attr_accessor :callback, :callback_queue, :callback_thread
  def callback(&block)
    if block_given?
    
      @callback = block

      @callback_queue = RbbtProcessSocket.new

      @callback_thread = Thread.new(Thread.current) do |parent|
        begin
          loop do
            p = @callback_queue.pop unless @callback_queue.cleaned

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
        rescue ClosedStream
        rescue Aborted
          Log.warn "Callback thread aborted"
          self._abort
          raise $!
        rescue Exception
          Log.warn "Exception captured in callback: #{$!.message}"
          self._abort
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
    @init_block = block

    @master_pid = Process.fork do
      @close_up = false

      Signal.trap(CLOSE_SIGNAL) do
        @close_up = true unless @closing_thread
        Misc.insist([0,0.01,0.1,0.2,0.5]) do
          raise TryAgain unless @manager_thread
          if @manager_thread.alive?
            raise "Manager thread for #{Process.pid} Not working yet" unless @manager_thread["working"]
            @manager_thread.raise TryAgain
          end
        end
      end

      Signal.trap(:USR1) do
        @count += 1
        @manager_thread.raise TryAgain
      end

      Signal.trap(:USR2) do
        @count -= 1
        @manager_thread.raise TryAgain
      end

      Signal.trap(ABORT_SIGNAL) do
        begin
          @monitor_thread.raise Aborted
        rescue Exception
          Log.exception $!
        end
      end


      if @callback_queue
        Misc.purge_pipes(@queue.swrite,@queue.sread,@callback_queue.swrite, @callback_queue.sread) 
      else
        Misc.purge_pipes(@queue.swrite,@queue.sread) 
      end

      @total = num_processes
      @count = 0
      @processes = []

      @manager_thread = Thread.new do
        begin
        while true 
          begin
            Thread.current["working"] = true
            if @close_up
              @close_up = false
              Log.debug "Closing up process queue #{Process.pid}"
              @count = 0
              @closing_thread = Thread.new do
                Log.debug "Pushing closed stream #{Process.pid}"
                while true
                  @queue.push ClosedStream.new unless @queue.cleaned 
                  break if @processes.empty?
                end unless @processes.empty?
              end
            end

            begin
              sleep 3
            rescue TryAgain
            end

            raise TryAgain if @close_up

            @process_mutex.synchronize do
              while @count > 0
                @count -= 1
                @total += 1
                @processes << RbbtProcessQueueWorker.new(@queue, @callback_queue, @cleanup, @respawn, @offset, &@init_block)
                Log.warn "Added process #{@processes.last.pid} to #{Process.pid} (#{@processes.length})"
              end

              while @count < 0
                @count += 1
                next unless @processes.length > 1
                first = @processes.shift
                first.stop
                Log.warn "Removed process #{first.pid} from #{Process.pid} (#{@processes.length})"
              end
            end
          rescue TryAgain
            retry
          rescue Aborted
            Log.low "Aborting manager thread #{Process.pid}"
            raise Aborted
          rescue Exception
            Log.exception $!
            raise Exception
          end
        end
        rescue Exception
          Log.exception $!
          raise $!
        end
      end

      @callback_queue.close_read if @callback_queue

      num_processes.times do |i|
        @process_mutex.synchronize do
          @processes << RbbtProcessQueueWorker.new(@queue, @callback_queue, @cleanup, @respawn, @offset, &@init_block)
        end
      end

      @monitor_thread = Thread.new do
        begin
          while @processes.any?
            @processes[0].join 
            @processes.shift
          end
        rescue Aborted
          Log.warn "Aborting process monitor"
          @processes.each{|p| p.abort_and_join}
          @processes.clear

          @callback_thread.kill if @callback_thread && @callback_thread.alive?
          @manager_thread.kill if @manager_thread.alive?
        rescue Exception
          Log.warn "Process monitor exception: #{$!.message}"
          @processes.each{|p| p.abort_and_join}
          @processes.clear

          @callback_thread.kill if @callback_thread && @callback_thread.alive?
          @manager_thread.kill if @manager_thread.alive?
        end
      end


      begin

        while @monitor_thread.alive?
          @monitor_thread.join(1)
        end
      rescue Exception
        Log.exception $!
      end

      Kernel.exit! 0
    end

    Log.info "Cpu process (#{num_processes}) started with master: #{@master_pid}"
    
    @queue.close_read
  end

  def add_process
    Process.kill :USR1, @master_pid
  end

  def remove_process
    Process.kill :USR2, @master_pid
  end

  def close_callback
    return unless @callback_thread.alive?
    begin
      t = Thread.new do
        @callback_queue.push ClosedStream.new
      end
    rescue Exception
      Log.warn "Error closing callback: #{$!.message}"
    end
    @callback_thread.join  if @callback_thread && @callback_thread.alive?
    t.join
  end

  def _join
    error = true
    begin
      pid, status = Process.waitpid2 @master_pid
      error = false if status.success?
      raise ProcessFailed if error
    rescue Errno::ECHILD
    rescue Aborted
      Log.error "Aborted joining queue"
      raise $!
    rescue Exception
      Log.error "Exception joining queue: #{$!.message}"
      raise $!
    ensure
      close_callback if @callback
      @callback_thread.join if @callback_thread
      if @join
        if @join.arity == 1
          @join.call(error) 
        else
          @join.call
        end
      end
    end

  end

  def join
    begin
      Process.kill CLOSE_SIGNAL, @master_pid
    rescue Errno::ECHILD, Errno::ESRCH
      Log.debug "Cannot kill #{@master_pid}: #{$!.message}"
    end 

    begin
      _join
    ensure
      close_callback if @callback
      @queue.swrite.close unless @queue.swrite.closed?
    end
    #@callback_thread.join if @callback_thread
    self.clean
  end

  def _abort
    begin
      Process.kill ABORT_SIGNAL, @master_pid
      pid, status = Process.waitpid2 @master_pid
    rescue Errno::ECHILD, Errno::ESRCH
      Log.warn "Cannot kill #{@master_pid}: #{$!.message}"
    end
  end

  def abort
    _abort
    (@callback_thread.raise(Aborted.new); @callback_thread.join) if @callback_thread and @callback_thread.alive?
    raise Aborted.new
  end

  def clean
    begin
      self.abort if Misc.pid_exists?(@master_pid)

    ensure
      @queue.clean if @queue
      @callback_queue.clean if @callback_queue
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
