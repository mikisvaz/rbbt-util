require 'rbbt/util/concurrency/processes/worker'
require 'rbbt/util/concurrency/processes/socket'

class RbbtProcessQueue

  attr_accessor :num_processes, :queue, :process_monitor, :cleanup, :join, :reswpan, :offset
  def initialize(num_processes, cleanup = nil, join = nil, reswpan = nil, offset = false)
    @num_processes = num_processes
    @cleanup = cleanup
    @join = join
    @respawn = reswpan
    @offset = offset
    @queue = RbbtProcessSocket.new

    key = "/" << rand(1000000000).to_s << '.' << Process.pid.to_s;
    @sem = key + '.process'
    Log.debug "Creating process semaphore: #{@sem}"
    RbbtSemaphore.create_semaphore(@sem,1)
  end


  ABORT_SIGNAL = :INT
  CLOSE_SIGNAL = :PIPE

  attr_accessor :callback, :callback_queue, :callback_thread
  def callback(&block)
    if block_given?
    
      @callback = block

      @callback_queue = RbbtProcessSocket.new

    else
      @callback, @callback_queue, @callback_thread = nil, nil, nil
    end
  end

  def init_master
    RbbtSemaphore.wait_semaphore(@sem)
    @master_pid = Process.fork do
      @close_up = false
      processes_initiated = false
      processes = []
      process_mutex = Mutex.new

      Signal.trap(CLOSE_SIGNAL) do
        if ! @closing_thread
          @close_up = true 
          Misc.insist([0,0.01,0.1,0.2,0.5]) do
            if ! @manager_thread
              Thread.pass 
              raise "Manager thread for #{Process.pid} not found yet" 
            end

            if @manager_thread.alive?
              raise "Manager thread for #{Process.pid} not working yet" unless @manager_thread["working"]
              @manager_thread.raise TryAgain
            end
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
        @abort_monitor = true
        @monitor_thread.raise Aborted if @monitor_thread && @monitor_thread.alive?
      end

      if @callback_queue
        Misc.purge_pipes(@queue.swrite, @queue.sread, @callback_queue.swrite) 
      else
        Misc.purge_pipes(@queue.swrite, @queue.sread) 
      end

      @total = 0
      @count = 0

      @manager_thread = Thread.new do
        while true 
          break if processes_initiated && processes.empty? && (@monitor_thread && ! @monitor_thread.alive?)
          begin
            Thread.current["working"] = true
            if @close_up
              Thread.handle_interrupt(TryAgain => :never) do
                Log.debug "Closing up process queue #{Process.pid}"
                @count = 0
                @closing_thread = Thread.new do
                  Thread.handle_interrupt(TryAgain => :never) do
                    Log.debug "Pushing closed stream #{Process.pid}"
                    while true
                      @queue.push ClosedStream.new unless @queue.cleaned 
                      break if processes_initiated && processes.empty?
                    end unless processes_initiated && processes.empty?
                  end
                end
                @close_up = false
              end
            end

            begin
              sleep 3
            rescue TryAgain
            end

            raise TryAgain if @close_up

            process_mutex.synchronize do
              Thread.handle_interrupt(TryAgain => :never) do
                while @count > 0
                  @count -= 1
                  @total += 1
                  processes << RbbtProcessQueueWorker.new(@queue, @callback_queue, @cleanup, @respawn, (@offset ? @total : false), &@init_block)
                  Log.warn "Added process #{processes.last.pid} to #{Process.pid} (#{processes.length})"
                end

                while @count < 0
                  @count += 1
                  @total -= 1
                  next unless processes.length > 1
                  last = processes.reject{|p| p.stopping }.last
                  last.stop
                  Log.warn "Removed process #{last.pid} from #{Process.pid} (#{processes.length})"
                end
              end
            end
          rescue TryAgain
            retry
          rescue Aborted
            Log.low "Aborting manager thread #{Process.pid}"
            raise $!
          rescue Exception
            raise Exception
          end
        end
        Log.low "Manager thread stopped #{Process.pid}"
      end

      @callback_queue.close_read if @callback_queue

      num_processes.times do |i|
        @total += 1 
        process_mutex.synchronize do
          processes << RbbtProcessQueueWorker.new(@queue, @callback_queue, @cleanup, @respawn, (@offset ? @total : false), &@init_block)
        end
      end

      processes_initiated = true

      @monitor_thread = Thread.new do
        begin
          while processes.any?
            raise Aborted if @abort_monitor
            #processes[0].join
            #Log.debug "Joined process #{processes[0].pid} of queue #{Process.pid}"
            #processes.shift
            pid, status = Process.wait2
            Log.debug "Joined process #{pid} of queue #{Process.pid} (status: #{status})"
            processes.reject!{|p| p.pid == pid}
            raise ProcessFailed.new pid if not status.success?
          end
          Log.low "All processes completed #{Process.pid}"
        rescue Aborted
          Log.exception $!
          Log.low "Aborting process monitor #{Process.pid}"
          processes.each{|p|  p.abort_and_join}
          Log.low "Processes aborted #{Process.pid}"
          processes.clear

          @manager_thread.raise Aborted if @manager_thread.alive?
          raise Aborted, "Aborted monitor thread"
        rescue Exception
          Log.low "Process monitor exception [#{Process.pid}]: #{$!.message}"
          processes.each{|p| p.abort_and_join}
          Log.low "Processes aborted #{Process.pid}"
          processes.clear

          @manager_thread.raise $! if @manager_thread.alive?
          raise Aborted, "Aborted monitor thread with exception"
        end
      end

      RbbtSemaphore.post_semaphore(@sem)

      Log.low "Process monitor #{Process.pid} joining threads"
      begin
        @monitor_thread.join
        @manager_thread.raise TryAgain if @manager_thread.alive?
        @manager_thread.join 
        @callback_queue.push ClosedStream.new if @callback_queue
      rescue Exception
        Kernel.exit -1
      end
      Log.low "Process monitor #{Process.pid} threads joined successfully, now exit"

      Kernel.exit 0
    end

    @queue.close_read
    Log.info "Cpu process (#{num_processes}) started with master: #{@master_pid}"
  end

  def init(&block)
    @init_block = block

    init_master

    RbbtSemaphore.synchronize(@sem) do
    @callback_thread = Thread.new do
      begin
        loop do
          p = @callback_queue.pop unless @callback_queue.cleaned

          if Exception === p or (Array === p and Exception === p.first)
            e = Array === p ? p.first : p
            Log.low "Callback recieved exception from worker: #{e.message}" unless Aborted === e or ClosedStream === e
            raise e 
          end

          if @callback.arity == 0
            @callback.call
          else
            @callback.call p
          end
        end
      rescue ClosedStream
        Log.low "Callback thread closing"
      rescue Aborted
        Log.low "Callback thread aborted"
        raise $!
      rescue Exception
        Log.low "Exception captured in callback: #{$!.message}"
        raise $!
      end
    end if @callback_queue
    end

  end

  def add_process
    Process.kill :USR1, @master_pid
  end

  def remove_process
    Process.kill :USR2, @master_pid
  end

  def _join
    error = :redo 
    begin
      pid, @status = Process.waitpid2 @master_pid unless @status
      error = true unless @status.success?
      begin
        @callback_thread.join if @callback_thread
        raise ProcessFailed.new @master_pid unless @status.success?
      rescue
        exception = $!
        raise $!
      end
      error = false
    rescue Aborted, Interrupt
      exception = $!
      Log.exception $!
      error = true
      if @aborted
        raise $!
      else
        self.abort
        Log.high "Process queue #{@master_pid} aborted"
        retry
      end
    rescue Errno::ESRCH, Errno::ECHILD
      retry if Misc.pid_exists? @master_pid
      error = ! @status.success?
    ensure
      begin
        begin
          self.abort
        ensure
          _join 
        end if error == :redo

        begin
          @callback_thread.join 
        rescue Exception
        end

        Log.medium "Joining process queue #{"(error) " if error}#{@master_pid} #{@join}" 
        begin
          if @join
            if @join.arity == 1
              @join.call(error) 
            else
              @join.call
            end
          end
        end
      ensure
        self.clean
      end

      if exception
        raise exception 
      else
        raise "Process queue #{@master_pid} failed" 
      end if error
    end
  end

  def close_up_queue
    begin
      RbbtSemaphore.synchronize(@sem) do
        Process.kill CLOSE_SIGNAL, @master_pid
      end
    rescue Errno::ECHILD, Errno::ESRCH
      Log.debug "Cannot kill (CLOSE) #{@master_pid}: #{$!.message}"
    end if Misc.pid_exists? @master_pid 
  end

  def join
    close_up_queue

    _join
  end

  def _abort
    begin
      Log.warn "Aborting process queue #{@master_pid}"
      Process.kill ABORT_SIGNAL, @master_pid
    rescue Errno::ECHILD, Errno::ESRCH
      Log.debug "Cannot abort #{@master_pid}: #{$!.message}"
    end
  end

  def abort
    _abort
    @callback_thread.raise(Aborted.new) if @callback_thread and @callback_thread.alive?
    @aborted = true
    begin
      _join
    rescue
    end
  end

  def clean
    RbbtSemaphore.delete_semaphore(@sem)
    begin
      self.abort if Misc.pid_exists?(@master_pid)

    ensure
      @queue.clean if @queue
      #@callback_thread.push ClosedStream if @callback_thread && @callback_thread.alive?
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
