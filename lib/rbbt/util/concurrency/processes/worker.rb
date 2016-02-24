require 'rbbt/util/concurrency/processes/socket'
class RbbtProcessQueue
  class RbbtProcessQueueWorker
    attr_reader :pid, :queue, :callback_queue, :cleanup, :block

    class Respawn < Exception
      attr_accessor :payload 
      def initialize(payload = nil)
        @payload = payload
      end
    end

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
          raise p.first if Array === p and Exception === p.first
          begin
            res = @block.call *p
            @callback_queue.push res if @callback_queue
          rescue Respawn
            @callback_queue.push $!.payload 
            raise $!
          end
          raise Respawn if @stop
        end
        Kernel.exit! 0
      rescue Respawn
        Kernel.exit! 28
      rescue ClosedStream
      rescue Aborted, Interrupt
        Log.info "Worker #{Process.pid} aborted"
      rescue Exception
        Log.exception $!
        @callback_queue.push($!) if @callback_queue
        Kernel.exit! -1
      ensure
        @callback_queue.close_write if @callback_queue 
      end
      Kernel.exit! 0
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

      status = nil
      begin
        @current = Process.fork do
          run
        end
        @asked = false

        initial = Misc.memory_use(Process.pid)
        memory_cap = multiplier * initial
        Log.medium "Worker #{Process.pid} started with #{@current} -- initial: #{initial} - multiplier: #{multiplier} - cap: #{memory_cap}"

        @monitor_thread = Thread.new do
          begin
            while true
              current = Misc.memory_use(@current) 
              if current > memory_cap and not @asked
                Log.medium "Worker #{@current} for #{Process.pid} asked to respawn -- initial: #{initial} - multiplier: #{multiplier} - cap: #{memory_cap} - current: #{current}"
                RbbtSemaphore.synchronize(@callback_queue.write_sem) do
                  Process.kill "USR1", @current
                end
                @asked = true
              end
              sleep 3 + rand(5)
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
          @asked = false
          Log.high "Worker #{Process.pid} respawning to #{@current}"
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
        @callback_queue.close_write if @callback_queue 
      end

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
