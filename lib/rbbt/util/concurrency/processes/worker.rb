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
        begin
          Signal.trap(:INT){ 
            Kernel.exit! -1
          }

          @respawn = false
          Signal.trap(:USR1){ 
            @respawn = true
          }

          @stop = false
          Signal.trap(:USR2){ 
            @stop = true
          }

          @abort = false
          Signal.trap(20){ 
            @abort = true
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
              @callback_queue.push $!.payload if @callback_queue
              raise $!
            end

            raise Respawn if @respawn
            if @stop
              Log.high "Worker #{Process.pid} leaving"
              break
            end
          end
          Kernel.exit! 0
        rescue Respawn
          Kernel.exit! 28
        rescue ClosedStream
        rescue Interrupt,Aborted
          Log.high "Worker #{Process.pid} aborted"
        rescue SemaphoreInterrupted
          retry unless @stop 
          Log.high "Worker #{Process.pid} leaving"
        rescue Exception
          begin
            @callback_queue.push($!) if @callback_queue
          rescue
          end
          Kernel.exit! -1
        ensure
          @callback_queue.close_write if @callback_queue 
        end
      rescue Aborted
        Log.high "Worker #{Process.pid} aborted"
      end
      Kernel.exit! 0
    end

    def run_with_respawn(multiplier = nil)
      multiplier = case multiplier
                   when String
                     multiplier.to_s
                   when Numeric
                     multiplier.to_i
                   else
                     3
                   end

      status = nil
      begin

        initial = Misc.memory_use(Process.pid)
        memory_cap = multiplier * initial

        @asked = false
        @monitored = false
        @monitor_thread = Thread.new do
          begin
            while true
              @monitored = true

              current_mem = @current ? Misc.memory_use(@current) : 0
              if current_mem > memory_cap and not @asked
                Log.medium "Worker #{@current} for #{Process.pid} asked to respawn -- initial: #{initial} - multiplier: #{multiplier} - cap: #{memory_cap} - current: #{current_mem}"
                RbbtSemaphore.synchronize(@callback_queue.write_sem) do
                  Process.kill "USR1", @current if @current
                end
                @asked = true
              end
              sleep 2
            end
          rescue
            Log.exception $!
          end
        end

        while ! @monitored
          sleep 0.1
        end

        @current = nil
        Signal.trap(:INT){ 
          begin
            Process.kill :INT, @current if @current
          rescue Errno::ESRCH, Errno::ECHILD
          end
        }

        Signal.trap(:USR1){ 
          begin
            Process.kill :USR1, @current if @current
          rescue Errno::ESRCH, Errno::ECHILD
          end
        }

        Signal.trap(:USR2){ 
          begin
            Process.kill :USR2, @current if @current
          rescue Errno::ESRCH, Errno::ECHILD
          end
        }

        Signal.trap(20){ 
          begin
            Process.kill 20, @current if @current
          rescue Errno::ESRCH, Errno::ECHILD
          end
        }

        @current = Process.fork do
          run
        end

        Log.debug "Worker for #{Process.pid} started with pid #{@current} -- initial: #{initial} - multiplier: #{multiplier} - cap: #{memory_cap}"

        while true
          @prev = @current
          pid, status = Process.waitpid2 @current
          code = status.to_i >> 8 
          break unless code == 28
          @current = Process.fork do
            run
          end
          @asked = false
          Log.high "Worker #{Process.pid} respawning from #{@prev} to #{@current}"
        end
      rescue Aborted, Interrupt
        Log.high "Worker #{Process.pid} aborted. Current #{@current} #{Misc.pid_exists?(@current) ? "exists" : "does not exist"}"
        Process.kill "INT", @current if Misc.pid_exists? @current
        @callback_queue.close_write if @callback_queue 
        Kernel.exit! 0
      rescue Exception
        raise $!
      ensure
        @monitor_thread.kill if @monitor_thread
        Process.kill "INT", @current if Misc.pid_exists? @current
        @callback_queue.close_write if @callback_queue 
      end

      if status
        Kernel.exit! status.to_i >> 8
      else
        Kernel.exit! -1
      end
    end

    def initialize(queue, callback_queue = nil, cleanup = nil, respawn = false, offset = false, &block)
      @queue, @callback_queue, @cleanup, @block, @offset = queue, callback_queue, cleanup, block, offset

      @pid = Process.fork do
        Misc.pre_fork
        Log::ProgressBar.add_offset if @offset

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
        Log::ProgressBar.remove_offset if @offset
      end
    end

    def join
      return unless Misc.pid_exists? @pid
      begin
        pid, status = Process.waitpid2 @pid
        raise ProcessFailed.new @pid if not status.success?
      rescue Aborted
        self.abort
        raise Aborted
      rescue Errno::ESRCH, Errno::ECHILD
        Log.exception $!
      rescue ProcessFailed
        raise $!
      rescue Exception
        Log.exception $!
        raise $!
      end
    end


    def abort
      begin
        Process.kill 20, @pid
      rescue Errno::ESRCH, Errno::ECHILD
      end
    end

    def abort_and_join
      self.abort

      begin
      Misc.insist([0,0.05,0.5,1,2]) do
        begin
          pid, status = Process.waitpid2 @pid, Process::WNOHANG
          Log.low "Abort and join of #{@pid}"
          return
        rescue Aborted
          abort
          raise 
        rescue ProcessFailed
          Log.low "Abort and join of #{@pid} (ProcessFailed)"
          return
        rescue Errno::ESRCH, Errno::ECHILD
          Log.low "Already joined worker #{@pid}"
          return
        end
      end
      rescue Aborted
        retry
      end

      begin
        Log.low "Forcing abort of #{@pid}"
        Process.kill 9, @pid
        pid, status = Process.waitpid2 @pid
      rescue Errno::ESRCH, Errno::ECHILD
        Log.low "Force killed worker #{@pid}"
      end
    end

    def stop
      begin
        Process.kill :USR2, @pid
      rescue Errno::ESRCH 
      rescue Exception
        Log.exception $!
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
