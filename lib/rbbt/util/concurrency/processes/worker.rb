require 'rbbt/util/concurrency/processes/socket'
class RbbtProcessQueue
  class RbbtProcessQueueWorker
    attr_reader :pid, :queue, :callback_queue, :cleanup, :block
    def initialize(queue, callback_queue = nil, cleanup = nil, &block)
      @queue, @callback_queue, @cleanup, @block = queue, callback_queue, cleanup, block

      @pid = Process.fork do
        begin
          @cleanup.call if @cleanup
          @queue.close_write 

          if @callback_queue
            Misc.purge_pipes(@callback_queue.swrite) 
            @callback_queue.close_read 
          else
            Misc.purge_pipes
          end

          Signal.trap(:INT){ 
            Kernel.exit! -1
          }

          loop do
            p = @queue.pop
            next if p.nil?
            raise p if Exception === p
            raise p.first if Exception === p.first
            res = @block.call *p
            @callback_queue.push res if @callback_queue
          end
          Kernel.exit! 0
        rescue ClosedStream
        rescue Aborted, Interrupt
          Log.warn "Worker #{Process.pid} aborted"
          Kernel.exit! -1
        rescue Exception
          @callback_queue.push($!) if @callback_queue
          Kernel.exit! -1
        ensure
          @callback_queue.close_write if @callback_queue 
        end
      end
    end

    def join
      joined_pid = Process.waitpid @pid
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
