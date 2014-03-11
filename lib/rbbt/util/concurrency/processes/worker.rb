require 'rbbt/util/concurrency/processes/socket'
class RbbtProcessQueue
  class RbbtProcessQueueWorker
    attr_accessor :pid, :queue, :callback_queue, :block
    def initialize(queue, callback_queue = nil, &block)
      @queue, @callback_queue, @block = queue, callback_queue, block

      @pid = Process.fork do
        begin
          @queue.swrite.close
          @callback_queue.sread.close if @callback_queue

          Signal.trap(:INT){ raise Aborted; }
          loop do
            p = @queue.pop
            raise p if Exception === p
            res = @block.call p
            @callback_queue.push res if @callback_queue
          end

          exit 0
        rescue RbbtProcessQueue::RbbtProcessSocket::ClosedSocket
          exit 0
        rescue Aborted
          exit -1
        rescue Exception
          Log.exception $!
          @callback_queue.push($!) if @callback_queue
          exit -1
        end

      end
    end

    def join
      Process.waitpid @pid
    end

    def abort
      Process.kill :INT, @pid
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
