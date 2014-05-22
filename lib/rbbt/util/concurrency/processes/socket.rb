require 'rbbt/util/semaphore'

class RbbtProcessQueue
  class RbbtProcessSocket

    Serializer = Marshal

    attr_accessor :sread, :swrite, :write_sem, :read_sem
    def initialize
      @sread, @swrite = Misc.pipe

      key = "/" << rand(100000000).to_s;
      @write_sem = key + '.in'
      @read_sem = key + '.out'
      Log.warn "Creating socket semaphores: #{key}"
      RbbtSemaphore.create_semaphore(@write_sem,1)
      RbbtSemaphore.create_semaphore(@read_sem,1)
    end

    def clean
      @sread.close unless @sread.closed?
      @swrite.close unless @swrite.closed?
      Log.warn "Destroying socket semaphores: #{key}"
      RbbtSemaphore.delete_semaphore(@write_sem)
      RbbtSemaphore.delete_semaphore(@read_sem)
    end


    def dump(obj, stream)
      case obj
      when String
        payload = obj
        size_head = [payload.bytesize,"S"].pack 'La'
        str = size_head << payload
      else
        payload = Serializer.dump(obj)
        size_head = [payload.bytesize,"M"].pack 'La'
        str = size_head << payload
      end

      write_length = str.length
      #IO.select(nil, [stream])
      wrote = stream.write(str) 
      while wrote < write_length
        wrote += stream.write(str[wrote..-1]) 
      end
    end

    def load(stream)
      size_head = Misc.read_stream stream, 5

      size, type = size_head.unpack('La')

      begin
        payload = Misc.read_stream stream, size
        case type
        when "M"
          Serializer.load(payload)
        when "S"
          payload
        end
      rescue TryAgain
        retry
      end
    end

    def closed_read?
      @sread.closed?
    end

    def closed_write?
      @swrite.closed?
    end

    def close_write
      @swrite.close
    end

    def close_read
      @sread.close 
    end
    #{{{ ACCESSOR
  
    
    def push(obj)
      RbbtSemaphore.synchronize(@write_sem) do
        self.dump(obj, @swrite)
      end
    end

    def pop
      RbbtSemaphore.synchronize(@read_sem) do
        self.load(@sread)
      end
    end
  end
end
