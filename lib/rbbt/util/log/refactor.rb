module Log

  def self._ignore_stderr
    begin
      File.open('/dev/null', 'w') do |f|
        backup_stderr = STDERR.dup
        STDERR.reopen(f)
        begin
          yield
        ensure
          STDERR.reopen backup_stderr
          backup_stderr.close
        end
      end
    rescue Errno::ENOENT
      yield
    end
  end


  def self.ignore_stderr(&block)
    _ignore_stderr &block
  end

  def self._ignore_stdout
    begin
      File.open('/dev/null', 'w') do |f|
        backup_stdout = STDOUT.dup
        STDOUT.reopen(f)
        begin
          yield
        ensure
          STDOUT.reopen backup_stdout
          backup_stdout.close
        end
      end
    rescue Errno::ENOENT
      yield
    end
  end


  def self.ignore_stdout(&block)
    _ignore_stdout &block
  end

end
