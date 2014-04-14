class Step

  attr_reader :stream

  def get_stream
    @mutex.synchronize do
      @stream = begin
        IO === @result ? @result : nil
      ensure
        @result = nil
      end
    end
  end

  def _exec
    @exec = true if @exec.nil?
    @task.exec_in((bindings ? bindings : self), *@inputs)
  end

  def exec(no_load=false)
    dependencies.each{|dependency| dependency.exec(no_load) }
    @result = self._exec
    @result = @result.stream if TSV::Dumper === @result
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

  def run_dependencies(seen = [])
    seen << self.path
    dependencies.uniq.each{|dependency| 
      next if seen.include? dependency.path
      Log.info "#{Log.color :magenta, "Checking dependency"} #{Log.color :yellow, task.name.to_s || ""} => #{Log.color :yellow, dependency.task_name.to_s || ""} -- #{Log.color :blue, dependency.path}"
      begin
        dependency.relay_log self
        dependency.clean if not dependency.done? and (dependency.error? or dependency.aborted?)
        dependency.clean if dependency.streaming? and not dependency.running?
        #dependency.run_dependencies(seen)
        dependency.run(ENV["RBBT_NO_STREAM"] != 'true') unless dependency.result or dependency.done?
        seen << dependency.path
        seen.concat dependency.rec_dependencies.collect{|d| d.path} 
      rescue Exception
        backtrace = $!.backtrace
        set_info :backtrace, backtrace 
        log(:error, "Exception processing dependency #{Log.color :yellow, dependency.task.name.to_s} -- #{$!.class}: #{$!.message}")
        raise $!
      end
    }
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

          set_info :pid, Process.pid
          set_info :issued, Time.now

          log(:preparing, "Preparing job: #{Misc.fingerprint dependencies}")
          set_info :dependencies, dependencies.collect{|dep| [dep.task_name, dep.name]}

          run_dependencies

          set_info :inputs, Misc.remove_long_items(Misc.zip2hash(task.inputs, @inputs)) unless task.inputs.nil?

          set_info :started, (start_time = Time.now)
          log :started, "#{Log.color :green, "Starting task"} #{Log.color :yellow, task.name.to_s || ""} [#{Process.pid}]"

          begin
            result = _exec
          rescue Aborted
            log(:error, "Aborted")

            kill_children
            raise $!
          rescue Exception
            backtrace = $!.backtrace

            # HACK: This fixes an strange behaviour in 1.9.3 where some
            # backtrace strings are coded in ASCII-8BIT
            kill_children
            set_info :backtrace, backtrace 
            log(:error, "#{$!.class}: #{$!.message}")
            backtrace.each{|l| l.force_encoding("UTF-8")} if String.instance_methods.include? :force_encoding
            raise $!
          end

          result = prepare_result result, @task.description, info if IO === result and ENV["RBBT_NO_STREAM"]
          result = prepare_result result.stream, @task.description, info if TSV::Dumper === result and ENV["RBBT_NO_STREAM"]

          case result
          when IO
            result = Misc.read_stream(result) if ENV["RBBT_NO_STREAM"]

            log :streaming, "#{Log.color :magenta, "Streaming task result IO"} #{Log.color :yellow, task.name.to_s || ""} [#{Process.pid}]"
            ConcurrentStream.setup result do
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
            result.abort_callback = Proc.new do
              begin
                log :error, "#{Log.color :red, "ERROR -- streamming aborted"} #{Log.color :yellow, task.name.to_s || ""} [#{Process.pid}] -- #{path}" if status == :streaming
                stop_dependencies
                abort_stream
              rescue
                Log.exception $!
              ensure
                join
              end
            end
          when TSV::Dumper
            log :streaming, "#{Log.color :magenta, "Streaming task result TSV::Dumper"} #{Log.color :yellow, task.name.to_s || ""} [#{Process.pid}]"
            ConcurrentStream.setup result.stream do
              begin
                set_info :done, (done_time = Time.now)
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
                stop_dependencies
                abort_stream
              rescue
                Log.exception $!
              ensure
                join
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
    ensure
      join unless no_load
    end
  end

  def fork(semaphore = nil)
    raise "Can not fork: Step is waiting for proces #{@pid} to finish" if not @pid.nil? and not Process.pid == @pid and Misc.pid_exists?(@pid) and not done? and info[:forked]
    @pid = Process.fork do
      begin
        RbbtSemaphore.wait_semaphore(semaphore) if semaphore
        FileUtils.mkdir_p File.dirname(path) unless Open.exists? File.dirname(path)
        begin
          res = run
        rescue Aborted
          Log.debug{"Forked process aborted: #{path}"}
          log :aborted, "Aborted"
          raise $!
        rescue Exception
          Log.debug("Exception '#{$!.message}' caught on forked process: #{path}")
          raise $!
        ensure
          join
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
      dep.abort unless dep.done?
    end
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
    stream ||= @stream 
    if stream
      stream.abort if stream.respond_to? :abort
    end
  end

  def abort
    begin
      abort_pid
      stop_dependencies
      abort_stream
    ensure
      log(:aborted, "Job aborted")
    end
  end

  def join_stream
    stream = get_stream if @result
    if stream
      begin
        Misc.consume_stream stream
        stream.join if stream.respond_to? :join # and not stream.joined?
      rescue Exception
        stream.abort if stream.respond_to? :abort 
        self.abort
        raise $!
      end
    end
  end

  def join

    join_stream

    return if not Open.exists? info_file
    pid = @pid 

    Misc.insist [0.1, 0.2, 0.5, 1] do
      pid ||= info[:pid]
    end

    if pid.nil?
      dependencies.each{|dep| dep.join }
      self
    else
      begin
        Log.debug{"Waiting for pid: #{pid}"}
        Process.waitpid pid 
      rescue Errno::ECHILD
        Log.debug{"Process #{ pid } already finished: #{ path }"}
      end if Misc.pid_exists? pid
      pid = nil
      dependencies.each{|dep| dep.join }
      self
    end
    self
  end
end
