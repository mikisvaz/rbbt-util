
class RbbtProcessQueue
  class RbbtProcessQueueWorker
    attr_accessor :pid, :queue, :callback_queue, :block
    def initialize(queue, callback_queue = nil, &block)
      @queue, @callback_queue, @block = queue, callback_queue, block

      @pid = Process.fork do
        begin
          @queue.sin.close
          @callback_queue.sout.close if @callback_queue
          Signal.trap(:INT){ raise Aborted }
          loop do
            p = @queue.pop
            res = @block.call *p
            @callback_queue.push(Array === res ? res : [res]) if @callback_queue
          end
        rescue ClosedQueue, Aborted
        rescue Exception
          Log.exception $!
          @callback_queue.push($!) if @callback_queue
          exit -1
        ensure
          @queue.sout.close
          @callback_queue.sin.close if @callback_queue
        end
      end
    end

    def join
      Process.waitpid @pid
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

    def abort
      Process.kill :INT, @pid
    end
  end
end
