class RbbtProcessQueue
  class RbbtProcessSocket

    class ClosedSocket < Exception; end

    attr_accessor :sin, :sout, :in_lockfile, :out_lockfile
    def initialize(lockfile = nil)
      @sout, @sin = File.pipe

      lockfile ||= TmpFile.tmp_file

      @lockfile = lockfile
      @in_lockfile = lockfile + '.in'
      @out_lockfile = lockfile + '.out'
      raise "in_lockfile exists?" if File.exists? @in_lockfile
      raise "out_lockfile exists?" if File.exists? @in_lockfile
      FileUtils.touch @in_lockfile
      FileUtils.touch @out_lockfile
    end

    def self.serialize(obj)
      dump = nil
      begin
        case obj
        when TSV
          type = "T"
          info = obj.info
          info.delete_if{|k,v| v.nil?}
          dump = Marshal.dump([info, {}.merge(obj)])
        else
          type = "M"
          dump = Marshal.dump(obj)
        end
        payload = [type, dump].pack('A1a*')
        length = payload.bytesize
        #Log.info "Writing #{ length }"
        [length].pack('L') << payload
      rescue Exception
        Log.error "Serialize error for: #{Misc.fingerprint obj} - #{Misc.fingerprint dump}"
        raise $!
      end
    end

    def self.unserialize(str)
      begin
        c, dump = str.unpack("A1a*")
        case c
        when "M"
          return Marshal.load(dump)
        when "T"
          info, hash = Marshal.load(dump)
          return TSV.setup(hash, info)
        end
      rescue Exception
        Log.error "Unserialize error for: #{Misc.fingerprint str}"
        raise $!
      end
    end

    def read_sout(length)
      str = ""
      str << sout.readpartial(length-str.length) while str.length < length
      str
    end

    def write_sin(str)
      str_length = str.length
      wrote = 0
      wrote += sin.write_nonblock(str[wrote..-1]) while wrote < str_length
    end

    def push(obj)
      Filelock in_lockfile do
        payload = RbbtProcessSocket.serialize(obj)
        sin << payload
      end
    end


    def pop
      r = []

      payload = begin
                  Filelock out_lockfile do
                    raise ClosedQueue if sout.eof?
                    r,w,e = IO.select([sout], [], [], 1)
                    raise TryAgain if r.empty?

                    first_char = read_sout(4)
                    length = first_char.unpack('L').first
                    #Log.info "Reading #{ length }"
                    read_sout(length)
                  end
                rescue TryAgain
                  sleep 1
                end

      RbbtProcessSocket.unserialize(payload)
    end

    def pop
      loop do
        r,w,e = IO.select([sout], [], [], 1)
        next if r.empty?
        break
      end

      first_char = read_sout(4)
      length = first_char.unpack('L').first
      #Log.info "Reading #{ length }"
      read_sout(length)
    end
  rescue TryAgain
    sleep 1
  end

      RbbtProcessSocket.unserialize(payload)
    end

    def rest
      sin.close
      str = sout.read
      res = []

      while not str.empty?
        first_char = str[0]
        next if first_char.nil?
        length = first_char.unpack("C").first
        dump = str[1..length]
        res << Marshal.load(dump)
        str = str[length+1..-1]
      end

      res
    end

    def clean
      FileUtils.rm @in_lockfile if File.exists? @in_lockfile
      FileUtils.rm @out_lockfile if File.exists? @out_lockfile
      sin.close unless sin.closed?
      sout.close unless sout.closed?
    end
