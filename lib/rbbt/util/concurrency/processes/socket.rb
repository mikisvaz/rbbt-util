require 'rbbt/util/semaphore'

class RbbtProcessQueue
  class RbbtProcessSocket
    class ClosedSocket < Exception; end

    Serializer = Marshal

    attr_accessor :sread, :swrite, :write_sem, :read_sem
    def initialize
      @sread, @swrite = IO.pipe

      key = rand(100000).to_s;
      @write_sem = key + '.in'
      @read_sem = key + '.out'
      RbbtSemaphore.create_semaphore(@write_sem,1)
      RbbtSemaphore.create_semaphore(@read_sem,1)
    end

    def clean
      @sread.close unless @sread.closed?
      @swrite.close unless @swrite.closed?
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
      IO.select(nil, [stream])
      wrote = stream.write(str) 
      while wrote < write_length
        wrote += stream.write(str[wrote..-1]) 
      end
    end

    def read_stream(stream, size)
      str = nil
      while not str = stream.read(size)
        IO.select([stream],nil,nil,1) 
        raise ClosedSocket if stream.eof?
      end

      while str.length < size
        raise ClosedSocket if stream.eof?
        IO.select([stream],nil,nil,1)
        if new = stream.read(size-str.length)
          str << new
        end
      end
      str
    end

    def load(stream)
      size_head = read_stream stream, 5

      size, type = size_head.unpack('La')

      begin
        payload = read_stream stream, size
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


    def push(obj)
      begin
        RbbtSemaphore.synchronize(@write_sem) do
          self.dump(obj, @swrite)
        end
      rescue
        return ClosedSocket.new
      end
    end

    def pop
      begin
        RbbtSemaphore.synchronize(@read_sem) do
          self.load(@sread)
        end
      rescue IOError, ClosedSocket
        return ClosedSocket.new
      end
    end
  end
end
