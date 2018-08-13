require 'rbbt/util/semaphore'

class RbbtProcessQueue
  class RbbtProcessSocket

    attr_accessor :sread, :swrite, :write_sem, :read_sem, :cleaned
    def initialize(serializer = nil)
      @sread, @swrite = Misc.pipe

      @serializer = serializer || Marshal

      @key = "/" << rand(1000000000).to_s << '.' << Process.pid.to_s;
      @write_sem = @key + '.in'
      @read_sem = @key + '.out'
      Log.debug "Creating socket semaphores: #{@key}"
      RbbtSemaphore.create_semaphore(@write_sem,1)
      RbbtSemaphore.create_semaphore(@read_sem,1)
    end

    def clean
      @cleaned = true
      @sread.close unless @sread.closed?
      @swrite.close unless @swrite.closed?
      Log.low "Destroying socket semaphores: #{[@key] * ", "}"
      RbbtSemaphore.delete_semaphore(@write_sem)
      RbbtSemaphore.delete_semaphore(@read_sem)
    end


    def dump(obj, stream)
      case obj
      when String
        payload = obj
        size_head = [payload.bytesize,"C"].pack 'La'
        str = size_head << payload
      else
        payload = @serializer.dump(obj)
        size_head = [payload.bytesize,"S"].pack 'La'
        str = size_head << payload
      end

      write_length = str.length
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
        when "S"
          begin
            @serializer.load(payload)
          rescue Exception
            Log.exception $!
            raise $!
          end
        when "C"
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
      @swrite.close unless closed_write?
    end

    def close_read
      @sread.close unless closed_read?
    end
    #{{{ ACCESSOR
  
    
    def push(obj)
      RbbtSemaphore.synchronize(@write_sem) do
        multiple = MultipleResult === obj
        obj = Annotated.purge(obj)
        obj.extend MultipleResult if multiple
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
