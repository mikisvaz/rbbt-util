require 'rbbt/util/concurrency/processes/socket'
class RbbtProcessQueue
  class RbbtProcessQueueWorker
    attr_reader :pid, :queue, :callback_queue, :cleanup, :block

    class Respawn < Exception; end

    def run
      begin
        Signal.trap(:INT){ 
          Kernel.exit! -1
        }

        @stop = false
        Signal.trap(:USR1){ 
          @stop = true
        }

        loop do
          p = @queue.pop
          next if p.nil?
          raise p if Exception === p
          raise p.first if Exception === p.first
          res = @block.call *p
          @callback_queue.push res if @callback_queue
          raise Respawn if @stop
        end
        Kernel.exit! 0
      rescue Respawn
        Kernel.exit! 28
      rescue ClosedStream
      rescue Aborted, Interrupt
        Log.warn "Worker #{Process.pid} aborted"
        Kernel.exit! 0
      rescue Exception
        Log.exception $!
        @callback_queue.push($!) if @callback_queue
        Kernel.exit! -1
      ensure
        @callback_queue.close_write if @callback_queue 
      end
    end

    def run_with_respawn(multiplier = nil)
      multiplier = case multiplier
                   when String
                     multiplier.to_s
                   when Fixnum
                     multiplier.to_i
                   else
                     3
                   end

      begin
        @current = Process.fork do
          run
        end
        Log.warn "Worker #{Process.pid} started with #{@current}"

        @monitor_thread = Thread.new do
          begin
            while true
              current = Misc.memory_use(@current) 
              initial = Misc.memory_use(Process.pid)
              if current > multiplier * initial
                Process.kill "USR1", @current
              end
            end
          rescue
            Log.exception $!
          end
        end

        while true
          pid, status = Process.waitpid2 @current
          code = status.to_i >> 8 
          break unless code == 28
          @current = Process.fork do
            run
          end
          Log.warn "Worker #{Process.pid} respawning to #{@current}"
        end
      rescue Aborted, Interrupt
        Log.warn "Worker #{Process.pid} aborted"
        Kernel.exit! 0
        Process.kill "INT", @current 
      rescue Exception
        Log.exception $!
        raise $!
      ensure
        @monitor_thread.kill
        Process.kill "INT", @current if Misc.pid_exists? @current
      end

      @callback_queue.close_write if @callback_queue 

      if status
        Kernel.exit! status.to_i >> 8
      else
        Kernel.exit! -1
      end
    end

    def initialize(queue, callback_queue = nil, cleanup = nil, respawn = false, &block)
      @queue, @callback_queue, @cleanup, @block = queue, callback_queue, cleanup, block

      @pid = Process.fork do
        Misc.pre_fork

        @cleanup.call if @cleanup
        @queue.close_write 

        if @callback_queue
          Misc.purge_pipes(@callback_queue.swrite) 
          @callback_queue.close_read 
        else
          Misc.purge_pipes
        end

        if respawn
          run_with_respawn respawn
        else
          run
        end
      end
    end

    def join
      Process.waitpid @pid
      raise ProcessFailed unless $?.success?
    end

    def abort
      begin
        Process.kill :INT, @pid
      rescue
      end
    end

    def done?
      begin
        Process.waitpid @pid, Process::WNOHANG
      rescue Errno::ECHILD
        true
      rescue
        false
      end
    end
  end
end
