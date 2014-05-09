class Step

  attr_reader :stream, :dupped, :saved_stream

  STREAM_CACHE = {}
  STREAM_CACHE_MUTEX = Mutex.new
  def self.dup_stream(stream)
    case stream
    when IO, File
      return stream if stream.closed?

      STREAM_CACHE_MUTEX.synchronize do
        case current = STREAM_CACHE[stream]
        when nil
          Log.medium "Not duplicating stream #{ Misc.fingerprint(stream) }"
          STREAM_CACHE[stream] = stream
        when File
          if Open.exists? current.filename 
            Log.medium "Reopening file #{ Misc.fingerprint(current) }"
            Open.open(current.filename)
          else
            Log.medium "Duplicating file #{ Misc.fingerprint(current) }"
            new = current.dup
            new.rewind
            new
          end

        else
          Log.medium "Duplicating stream #{ Misc.fingerprint(stream) }"
          Misc.dup_stream(current)
        end
      end
    when TSV::Dumper#, TSV::Parser
      stream = stream.stream
      return stream if stream.closed?

      STREAM_CACHE_MUTEX.synchronize do
        if STREAM_CACHE[stream].nil?
          Log.high "Not duplicating dumper #{ stream.inspect }"
          STREAM_CACHE[stream] = stream
        else
          new = Misc.dup_stream(STREAM_CACHE[stream])
          Log.high "Duplicating dumper #{ stream.inspect } into #{new.inspect}"
          new
        end
      end
    else
      stream
    end
  end

  def self.purge_stream_cache
    return
    STREAM_CACHE_MUTEX.synchronize do
      STREAM_CACHE.collect{|k,s| 
        Thread.new do
          Misc.consume_stream s
        end
      }
      STREAM_CACHE.clear
    end
  end

  def get_stream
    @mutex.synchronize do
      begin
        return nil if @saved_stream
        if IO === @result 
          @saved_stream = @result 
        else 
          nil
        end
      end
    end
  end

  def dup_inputs
    return if @dupped or ENV["RBBT_NO_STREAM"] == 'true'
    @inputs = @inputs.collect do |input|
      Step.dup_stream input
    end
    @dupped = true
  end

  def _exec
    @exec = true if @exec.nil?
    @task.exec_in((bindings ? bindings : self), *@inputs)
  end

  def exec(no_load=false)
    dup_inputs
    dependencies.each{|dependency| dependency.exec(no_load) }
    @mutex.synchronize do
      @result = self._exec
      @result = @result.stream if TSV::Dumper === @result
    end
    no_load ? @result : prepare_result(@result, @task.result_description)
  end

  def checks
    rec_dependencies.collect{|dependency| dependency.path }.uniq
  end

  def kill_children
    children_pids = info[:children_pids]
    if children_pids and children_pids.any?
      Log.medium("Killing children: #{ children_pids * ", " }")
      children_pids.each do |pid|
        Log.medium("Killing child #{ pid }")
        begin
          Process.kill "INT", pid
        rescue Exception
          Log.medium("Exception killing child #{ pid }: #{$!.message}")
        end
      end
    end
  end

  def run_dependencies
    @seen ||= []
    dependencies.uniq.each do |dependency| 
      next if seen.collect{|d| d.path}.include?(dependency.path)
      dependency.seen = seen
      @seen << dependency
      @seen.concat dependency.rec_dependencies.collect{|d| d } 
      @seen.uniq!
    end

    @seen.each do |dependency|
      next if dependency == self
      next unless dependencies.include? dependency
      dependency.relay_log self
      dependency.dup_inputs
    end

    @seen.each do |dependency| 
      next if dependency == self
      next unless dependencies.include? dependency
      Log.info "#{Log.color :cyan, "dependency"} #{Log.color :yellow, task.name.to_s || ""} => #{Log.color :yellow, dependency.task_name.to_s || ""} -- #{Log.color :blue, dependency.path}"
      begin
        if dependency.streaming? 
          next if dependency.running?
          dependency.clean 
        else
          dependency.clean if (dependency.error? or dependency.aborted? or not dependency.done?)
        end

        unless dependency.result or dependency.done?
          dependency.run(true) 
        end
      rescue Aborted
        backtrace = $!.backtrace
        set_info :backtrace, backtrace 
        log(:error, "Aborted dependency #{Log.color :yellow, dependency.task.name.to_s}")
        self.abort
        raise $!
      rescue Interrupt
        backtrace = $!.backtrace
        set_info :backtrace, backtrace 
        self.abort
        log(:error, "Interrupted dependency #{Log.color :yellow, dependency.task.name.to_s}")
        raise $!
      rescue Exception
        backtrace = $!.backtrace
        set_info :backtrace, backtrace 
        log(:error, "Exception processing dependency #{Log.color :yellow, dependency.task.name.to_s} -- #{$!.class}: #{$!.message}")
        self.abort
        raise $!
      end
    end
  end

  def run(no_load = false)
    result = nil

    begin
      @mutex.synchronize do
        no_load = no_load ? :stream : false
        result = Persist.persist "Job", @task.result_type, :file => path, :check => checks, :no_load => no_load do |lockfile|
          if Step === Step.log_relay_step and not self == Step.log_relay_step
            relay_log(Step.log_relay_step) unless self.respond_to? :relay_step and self.relay_step
          end
          @exec = false

          Open.rm info_file if Open.exists? info_file

          log :setup, "#{Log.color :magenta, "Task"} #{Log.color :yellow, task.name.to_s || ""}"

          merge_info({
            :pid => Process.pid,
            :issued => Time.now,
            :name => name,
            :dependencies => dependencies.collect{|dep| [dep.task_name, dep.name, dep.path]},
          })

          dup_inputs
          begin
            run_dependencies
          rescue
            log(:error, "Error procesing dependencies")
            stop_dependencies
            raise $!
          end


          set_info :inputs, Misc.remove_long_items(Misc.zip2hash(task.inputs, @inputs)) unless task.inputs.nil?

          set_info :started, (start_time = Time.now)
          log :started, "#{Log.color :green, "Starting task"} #{Log.color :yellow, task.name.to_s || ""} [#{Process.pid}]"

          begin
            result = _exec
          rescue Aborted
            stop_dependencies
            log(:error, "Aborted")
            raise $!
          rescue Exception
            backtrace = $!.backtrace

            # HACK: This fixes an strange behaviour in 1.9.3 where some
            # backtrace strings are coded in ASCII-8BIT
            set_info :backtrace, backtrace 
            log(:error, "#{$!.class}: #{$!.message}")
            backtrace.each{|l| l.force_encoding("UTF-8")} if String.instance_methods.include? :force_encoding
            stop_dependencies
            raise $!
          end

          if not no_load or ENV["RBBT_NO_STREAM"] == "true" 
            result = prepare_result result, @task.description, info if IO === result 
            result = prepare_result result.stream, @task.description, info if TSV::Dumper === result 
          end

          case result
          when IO

            log :streaming, "#{Log.color :magenta, "Streaming IO"} #{Log.color :yellow, task.name.to_s || ""} [#{Process.pid}]"
            ConcurrentStream.setup result do
              begin
                set_info :done, (done_time = Time.now)
                set_info :time_elapsed, (time_elapsed = done_time - start_time)
                log :done, "#{Log.color :red, "Completed"} #{Log.color :yellow, task.name.to_s || ""} [#{Process.pid}] +#{time_elapsed.to_i} -- #{path}"
              rescue
                Log.exception $!
              ensure
                join
              end
            end
            result.abort_callback = Proc.new do
              begin
                log :error, "#{Log.color :red, "ERROR -- streamming aborted"} #{Log.color :yellow, task.name.to_s || ""} [#{Process.pid}] -- #{path}" if status == :streaming
              rescue
                Log.exception $!
              ensure
                join
              end
            end
          when TSV::Dumper
            log :streaming, "#{Log.color :magenta, "Streaming TSV::Dumper"} #{Log.color :yellow, task.name.to_s || ""} [#{Process.pid}]"
            ConcurrentStream.setup result.stream do
              begin
                set_info :done, (done_time = Time.now)
                set_info :time_elapsed, (time_elapsed = done_time - start_time)
                log :done, "#{Log.color :red, "Completed task"} #{Log.color :yellow, task.name.to_s || ""} [#{Process.pid}] +#{time_elapsed.to_i} -- #{path}"
              rescue
                Log.exception $!
              ensure
                join
              end
            end
            result.stream.abort_callback = Proc.new do
              begin
                log :error, "#{Log.color :red, "ERROR -- streamming aborted"} #{Log.color :yellow, task.name.to_s || ""} [#{Process.pid}] -- #{path}"  if status == :streaming
              rescue Exception
                Log.exception $!
              end
            end
          else
            set_info :done, (done_time = Time.now)
            set_info :time_elapsed, (time_elapsed = done_time - start_time)
            log :done, "#{Log.color :red, "Completed task"} #{Log.color :yellow, task.name.to_s || ""} [#{Process.pid}] +#{time_elapsed.to_i}"
          end

          result
        end

        if no_load
          @result ||= result
          self
        else
          @result = prepare_result result, @task.result_description
        end
      end
    rescue Exception
      self.abort
      raise $!
    end
  end

  def fork(semaphore = nil)
    raise "Can not fork: Step is waiting for proces #{@pid} to finish" if not @pid.nil? and not Process.pid == @pid and Misc.pid_exists?(@pid) and not done? and info[:forked]
    @pid = Process.fork do
      begin
        RbbtSemaphore.wait_semaphore(semaphore) if semaphore
        FileUtils.mkdir_p File.dirname(path) unless Open.exists? File.dirname(path)
        begin
          res = run true
        rescue Aborted
          Log.debug{"Forked process aborted: #{path}"}
          log :aborted, "Job aborted (#{Process.pid})"
          raise $!
        rescue Exception
          Log.debug("Exception '#{$!.message}' caught on forked process: #{path}")
          raise $!
        ensure
          join_stream
        end

        begin
          children_pids = info[:children_pids]
          if children_pids
            children_pids.each do |pid|
              if Misc.pid_exists? pid
                begin
                  Process.waitpid pid
                rescue Errno::ECHILD
                  Log.low "Waiting on #{ pid } failed: #{$!.message}"
                end
              end
            end
            set_info :children_done, Time.now
          end
        rescue Exception
          Log.debug("Exception waiting for children: #{$!.message}")
          exit -1
        end
        set_info :pid, nil
        exit 0
      ensure
        RbbtSemaphore.post_semaphore(semaphore) if semaphore
      end
    end
    set_info :forked, true
    Process.detach(@pid)
    self
  end

  def stop_dependencies
    dependencies.each do |dep|
      dep.abort
    end
    kill_children
  end

  def abort_pid
    @pid ||= info[:pid]

    case @pid
    when nil
      Log.medium "Could not abort #{path}: no pid"
      false
    when Process.pid
      Log.medium "Could not abort #{path}: same process"
      false
    else
      Log.medium "Aborting #{path}: #{ @pid }"
      begin
        Process.kill("KILL", @pid)
        Process.waitpid @pid
      rescue Exception
        Log.debug("Aborted job #{@pid} was not killed: #{$!.message}")
      end
      true
    end
  end

  def abort_stream
    stream = get_stream if @result
    stream ||= @saved_stream 
    @saved_stream = nil
    if stream and stream.respond_to? :abort and not stream.aborted?
      begin
        Log.medium "Aborting job stream #{stream.inspect} -- #{Log.color :blue, path}"
        stream.abort 
        #stream.close unless stream.closed?
      rescue Aborted
        Log.medium "Aborting job stream #{stream.inspect} ABORTED RETRY -- #{Log.color :blue, path}"
        retry
      end
    end
  end

  def abort
    return if @aborted
    @aborted = true
    return if done?
    Log.medium{"#{Log.color :red, "Aborting"} #{Log.color :blue, path}"}
    begin
      stop_dependencies
      abort_stream
      abort_pid
    rescue Aborted
      Log.medium{"#{Log.color :red, "Aborting ABORTED RETRY"} #{Log.color :blue, path}"}
      retry
    rescue Exception
      retry
    ensure
      if Open.exists? path
        Log.warn "Aborted job had finished. Removing result"
        begin
          Open.rm path
        rescue Exception
          Log.warn "Exception removing result of aborted job: #{$!.message}"
        end
      end

      begin
        log(:aborted, "Job aborted")
      rescue Exception
        Log.exception $!
      end
    end
    Log.medium{"#{Log.color :red, "Aborted"} #{Log.color :blue, path}"}
  end

  def join_stream
    stream = get_stream if @result
    if stream
      begin
        Misc.consume_stream stream
      rescue Exception
        self.abort
        raise $!
      end
    end
  end

  def grace
    until done? or result or streaming? or error? or aborted?
      sleep 1 
    end
    self
  end

  def join

    grace

    join_stream if status == :streaming

    return self if not Open.exists? info_file

    return self if info[:joined]
    pid = @pid 

    Misc.insist [0.1, 0.2, 0.5, 1] do
      pid ||= info[:pid]
    end

    begin
      if pid.nil? or Process.pid == pid
        dependencies.each{|dep| dep.join }
        while not done?
          sleep 1
        end
      else
        begin
          Log.debug{"Waiting for pid: #{pid}"}
          Process.waitpid pid 
        rescue Errno::ECHILD
          Log.debug{"Process #{ pid } already finished: #{ path }"}
        end if Misc.pid_exists? pid
        pid = nil
        dependencies.each{|dep| dep.join }
      end
      self
    ensure
      set_info :joined, true
    end
  end
end
