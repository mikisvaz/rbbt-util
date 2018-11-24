require 'rbbt/workflow/step/dependencies'

class Step

  attr_reader :stream, :dupped, :saved_stream

  def get_stream
    @mutex.synchronize do
      Log.low "Getting stream from #{path} #{!@saved_stream} [#{object_id}-#{Misc.fingerprint(@result)}]"
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

  def resolve_input_steps
    step = false
    pos = 0

    input_options = Workflow === workflow ? workflow.task_info(task_name)[:input_options] : {}
    new_inputs = inputs.collect do |i| 
      begin
        if Step === i
          if i.error?
            e = i.get_exception
            if e
              raise e
            else
              raise DependencyError, "Error in dep. #{Log.blue e.path}"
            end
          end
          step = true
          i.produce unless i.done? || i.error? || i.started?
          if i.done?
            if (task.input_options[task.inputs[pos]] || {})[:stream]
              TSV.get_stream i
            else
              if (task.input_options[task.inputs[pos]] || {})[:nofile]
                i.path
              else
                i.load
              end
            end
          elsif i.streaming? and (task.input_options[task.inputs[pos]] || {})[:stream]
            TSV.get_stream i
          else
            i.join
            if (task.input_options[task.inputs[pos]] || {})[:stream]
              TSV.get_stream i
            else
              if (task.input_options[task.inputs[pos]] || {})[:nofile]
                i.path
              else
                i.load
              end
            end
          end
        else
          i
        end
      ensure
        pos += 1
      end
    end
    @inputs.replace new_inputs if step
  end

  def rewind_inputs
    return if @inputs.nil?
    Log.debug "Rewinding inputs for #{path}"
    @inputs.each do |input|
      next unless input.respond_to? :rewind
      begin
        input.rewind
        Log.debug "Rewinded #{Misc.fingerprint input}"
      rescue
      end
    end
  end

  def _exec
    resolve_input_steps
    rewind_inputs
    @exec = true if @exec.nil?
    begin
      old = Signal.trap("INT"){ Thread.current.raise Aborted }
      @task.exec_in((bindings ? bindings : self), *@inputs)
    ensure
      Signal.trap("INT", old)
    end
  end

  def exec(no_load=false)
    dependencies.each{|dependency| dependency.exec(no_load) }
    @mutex.synchronize do
      @result = self._exec
      @result = @result.stream if TSV::Dumper === @result
    end
    (no_load or ENV["RBBT_NO_STREAM"]) ? @result : prepare_result(@result, @task.result_description)
  end

  def updatable?
    (ENV["RBBT_UPDATE_ALL_JOBS"] == 'true' || Open.exists?(info_file)) && ! relocated?
  end

  def dependency_checks
    rec_dependencies.
      select{|dependency| ! (defined? WorkflowRESTClient and WorkflowRESTClient::RemoteStep === dependency) }.
      select{|dependency| ! Open.remote?(dependency.path) }.
      select{|dependency| dependency.updatable? }.
      select{|dependency| ! dependency.error? }
  end

  def input_checks
    inputs.select{|i| Step === i }.
      select{|dependency| dependency.updatable? }
  end

  def checks
    (dependency_checks + input_checks).uniq
  end

  def persist_checks
    canfail_paths = self.canfail_paths
    checks.collect do |dep| 
      path = dep.path
      next if ! dep.done? && canfail_paths.include?(path)
      path 
    end.compact
  end

  def out_of_date

    checks = self.checks
    return [] if checks.empty?
    outdated_time  = []
    outdated_dep  = []
    canfail_paths = self.canfail_paths
    this_mtime = Open.mtime(self.path) if Open.exists?(self.path)

    checks.each do |dep| 
      next unless dep.updatable?
      dep_done = dep.done?

      begin
        if this_mtime && dep_done && Open.exists?(dep.path) && (Open.mtime(dep.path) > this_mtime)
          outdated_time << dep
        end
      rescue
      end

      # Is this pointless? this would mean some dep got updated after a later
      # dep but but before this one.
      #if (! dep.done? && ! canfail_paths.include?(dep.path)) || ! dep.updated?

      if (! dep_done && ! canfail_paths.include?(dep.path))
        outdated_dep << dep
      end
    end

    Log.high "Some newer files found: #{Misc.fingerprint outdated_time}" if outdated_time.any?
    Log.high "Some outdated files found: #{Misc.fingerprint outdated_dep}" if outdated_dep.any?

    outdated_time + outdated_dep
  end

  def updated?
    return true unless (done? || error? || ! writable?)

    @updated ||= out_of_date.empty?
  end

  def kill_children
    begin
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
    rescue
      Log.medium("Exception finding children")
    end
  end

  def run(no_load = false)
    result = nil

    begin
      time_elapsed = total_time_elapsed = nil
      res = @mutex.synchronize do
        no_load = :stream if no_load

        Open.write(pid_file, Process.pid.to_s) unless Open.exists?(path) or Open.exists?(pid_file)
        result = Persist.persist "Job", @task.result_type, :file => path, :check => persist_checks, :no_load => no_load do 
          if Step === Step.log_relay_step and not self == Step.log_relay_step
            relay_log(Step.log_relay_step) unless self.respond_to? :relay_step and self.relay_step
          end

          Open.write(pid_file, Process.pid.to_s) unless Open.exists? pid_file

          @exec = false
          init_info

          log :setup, "#{Log.color :green, "Setup"} step #{Log.color :yellow, task.name.to_s || ""}"

          merge_info({
            :issued => (issue_time = Time.now),
            :name => name,
            :pid => Process.pid.to_s,
            :clean_name => clean_name,
            :workflow => (@workflow || @task.workflow).to_s,
            :task_name => @task.name,
            :result_type => @task.result_type,
            :result_description => @task.result_description,
            :dependencies => dependencies.collect{|dep| [dep.task_name, dep.name, dep.path]}
          })


          begin
            run_dependencies
          rescue Exception
            Open.rm pid_file if Open.exists?(pid_file)
            stop_dependencies
            raise $!
          end

          new_inputs = []
          @inputs.each_with_index do |input,i|
            name = @task.inputs[i]
            type = @task.input_types[name]

            if type == :directory
              directory_inputs = file('directory_inputs')
              input_source = directory_inputs['.source'][name].find
              input_dir = directory_inputs[name].find

              case input
              when Path
                if input.directory?
                  new_inputs << input
                else
                  input.open do |io|
                    begin
                      Misc.untar(io, input_source)
                    rescue
                      raise ParameterException, "Error unpackaging tar directory input '#{name}':\n\n#{$!.message}"
                    end
                  end
                  tar_1 = input_source.glob("*")
                  raise ParameterException, "When using tar.gz files for directories, the directory must be the single first level entry" if tar_1.length != 1
                  FileUtils.ln_s Misc.path_relative_to(directory_inputs, tar_1.first), input_dir
                  new_inputs << input_dir
                end
              when File, IO, Tempfile
                begin
                  Misc.untar(Open.gunzip(input), input_source)
                rescue
                  raise ParameterException, "Error unpackaging tar directory input '#{name}':\n\n#{$!.message}"
                end
                tar_1 = input_source.glob("*")
                raise ParameterException, "When using tar.gz files for directories, the directory must be the single first level entry" if tar_1.length != 1
                FileUtils.ln_s Misc.path_relative_to(directory_inputs, tar_1.first), input_dir
                new_inputs << input_dir
              else
                raise ParameterException, "Format of directory input '#{name}' not understood: #{Misc.fingerprint input}"
              end
            else
              new_inputs << input
            end
          end if @inputs

          @inputs = new_inputs if @inputs

          if not task.inputs.nil?
            info_inputs = @inputs.collect do |i| 
              if Path === i 
                i.to_s
              else 
                i 
              end
            end
            set_info :inputs, Misc.remove_long_items(Misc.zip2hash(task.inputs, info_inputs)) 
          end

          set_info :started, (start_time = Time.now)
          log :started, "Starting step #{Log.color :yellow, task.name.to_s || ""}"

          begin
            result = _exec
          rescue Aborted, Interrupt
            log(:aborted, "Aborted")
            raise $!
          rescue Exception
            backtrace = $!.backtrace

            # HACK: This fixes an strange behaviour in 1.9.3 where some
            # backtrace strings are coded in ASCII-8BIT
            backtrace.each{|l| l.force_encoding("UTF-8")} if String.instance_methods.include? :force_encoding
            set_info :backtrace, backtrace 
            log(:error, "#{$!.class}: #{$!.message}")
            stop_dependencies
            raise $!
          end

          if not no_load or ENV["RBBT_NO_STREAM"] == "true" 
            result = prepare_result result, @task.description, info if IO === result 
            result = prepare_result result.stream, @task.description, info if TSV::Dumper === result 
          end

          stream = case result
                   when IO
                     result
                   when TSV::Dumper
                     result.stream
                   end

          if stream
            log :streaming, "Streaming step #{Log.color :yellow, task.name.to_s || ""}"

            callback = Proc.new do
              if AbortedStream === stream
                if stream.exception
                  raise stream.exception 
                else
                  raise Aborted
                end
              end
              begin
                status = self.status
                if status != :done and status != :error and status != :aborted
                  Misc.insist do
                    merge_info({
                      :done => (done_time = Time.now),
                      :total_time_elapsed => (total_time_elapsed = done_time - issue_time),
                      :time_elapsed => (time_elapsed = done_time - start_time)
                    })
                    log :done, "Completed step #{Log.color :yellow, task.name.to_s || ""} in #{time_elapsed.to_i}+#{(total_time_elapsed - time_elapsed).to_i} sec."
                  end
                end
              rescue
                Log.exception $!
              ensure
                Step.purge_stream_cache
                set_info :pid, nil
                Open.rm pid_file if Open.exist?(pid_file)
              end
            end

            abort_callback = Proc.new do |exception|
              begin
                if exception
                  self.exception exception
                else
                  log :aborted, "#{Log.color :red, "Aborted"} step #{Log.color :yellow, task.name.to_s || ""}" if status == :streaming
                end
                _clean_finished
              rescue
                stop_dependencies
                set_info :pid, nil
                Open.rm pid_file if Open.exist?(pid_file)
              end
            end

            ConcurrentStream.setup stream, :callback => callback, :abort_callback => abort_callback

            if AbortedStream === stream 
              exception = stream.exception || Aborted.new("Aborted stream: #{Misc.fingerprint stream}")
              self.exception exception
              _clean_finished
              raise exception
            end
          else
            merge_info({
              :done => (done_time = Time.now),
              :total_time_elapsed => (total_time_elapsed = done_time - issue_time),
              :time_elapsed => (time_elapsed = done_time - start_time)
            })
            log :ending
            Step.purge_stream_cache
            Open.rm pid_file if Open.exist?(pid_file)
          end

          set_info :dependencies, dependencies.collect{|dep| [dep.task_name, dep.name, dep.path]}

          if result.nil? && File.exists?(self.tmp_path) && ! File.exists?(self.path)
            Open.mv self.tmp_path, self.path
          end
          result
        end # END PERSIST
        log :done, "Completed step #{Log.color :yellow, task.name.to_s || ""} in #{time_elapsed.to_i}+#{(total_time_elapsed - time_elapsed).to_i} sec." unless stream or time_elapsed.nil?

        if no_load
          @result ||= result
          self
        else
          Step.purge_stream_cache
          @result = prepare_result result, @task.result_description
        end
      end # END SYNC
      res
    rescue DependencyError
      exception $!
    rescue LockInterrupted
      raise $!
    rescue Aborted, Interrupt
      abort
      stop_dependencies
      raise $!
    rescue Exception
      exception $!
      stop_dependencies
      raise $!
    ensure 
      no_load = false unless IO === result
      Open.rm pid_file if Open.exist?(pid_file) unless no_load
      set_info :pid, nil unless no_load
    end
  end

  def produce(force=false, dofork=false)
    return self if done? and not dirty?

    self.status_lock.synchronize do
      if error? || aborted? || stalled?
        if stalled?
          Log.warn "Aborting stalled job #{self.path}"
          abort
        end
        if force or aborted? or recoverable_error?
          clean
        else
          e = get_exception
          if e
            Log.error "Raising exception in produced job #{self.path}: #{e.message}" 
            raise e
          else
            raise "Error in job: #{self.path}"
          end
        end
      end
    end

    update if done?

    if dofork
      fork(true) unless started?

      join unless done?
    else
      run(true) unless started?

      join unless done?
    end

    self
  end

  def fork(no_load = false, semaphore = nil)
    raise "Can not fork: Step is waiting for proces #{@pid} to finish" if not @pid.nil? and not Process.pid == @pid and Misc.pid_exists?(@pid) and not done? and info[:forked]
    sout, sin = Misc.pipe if no_load == :stream
    @pid = Process.fork do
      sout.close if sout
      Misc.pre_fork
      begin
        RbbtSemaphore.wait_semaphore(semaphore) if semaphore
        Open.mkdir File.dirname(path) unless Open.exist?(File.dirname(path))
        begin
          @forked = true
          res = run no_load
          set_info :forked, true
          if sin
            io = TSV.get_stream res
            if io.respond_to? :setup
              io.setup(sin) 
              sin.pair = io
              io.pair = sin
            end
            begin
              Misc.consume_stream(io, false, sin)
            rescue 
              Log.warn "Could not consume stream (#{io.closed? ? 'closed' : 'open'}) into pipe for forked job: #{self.path}"
              Misc.consume_stream(io) unless io.closed?
            end
          end
        rescue Aborted, Interrupt
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
          RbbtSemaphore.post_semaphore(semaphore) if semaphore
          Kernel.exit! -1
        end
        set_info :pid, nil
      ensure
        RbbtSemaphore.post_semaphore(semaphore) if semaphore
        Kernel.exit! 0
      end
    end
    sin.close if sin
    @result = sout if sout 
    Process.detach(@pid)
    self
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
      Log.medium "Aborting pid #{path}: #{ @pid }"
      begin
        Process.kill("INT", @pid)
        Process.waitpid @pid
      rescue Exception
        Log.debug("Aborted job #{@pid} was not killed: #{$!.message}")
      end
      Log.medium "Aborted pid #{path}: #{ @pid }"
      true
    end
  end

  def abort_stream
    stream = @result if IO === @result
    @saved_stream = nil
    if stream and stream.respond_to? :abort and not stream.aborted?
      doretry = true
      begin
        Log.medium "Aborting job stream #{stream.inspect} -- #{Log.color :blue, path}"
        stream.abort 
      rescue Aborted, Interrupt
        Log.medium "Aborting job stream #{stream.inspect} ABORTED RETRY -- #{Log.color :blue, path}"
        if doretry
          doretry = false
          retry
        end
      end
    end
  end

  def _clean_finished
    if Open.exists? path and not status == :done
      Log.warn "Aborted job had finished. Removing result -- #{ path }"
      begin
        Open.rm path
      rescue Exception
        Log.warn "Exception removing result of aborted job: #{$!.message}"
      end
    end
  end

  def _abort
    return if @aborted
    @aborted = true
    Log.medium{"#{Log.color :red, "Aborting"} #{Log.color :blue, path}"}
    doretry = true
    begin
      return if done?
      abort_pid if running?
      abort_stream
      stop_dependencies
    rescue Aborted, Interrupt
      Log.medium{"#{Log.color :red, "Aborting ABORTED RETRY"} #{Log.color :blue, path}"}
      if doretry
        doretry = false
        retry
      end
      raise $!
    rescue Exception
      if doretry
        doretry = false
        retry
      end
    ensure
      _clean_finished
    end
  end

  def abort
    return if done? and (status == :done or status == :noinfo)
    _abort
    log(:aborted, "Job aborted") unless aborted? or error?
    self
  end

  def join_stream
    stream = get_stream if @result
    @result = nil
    if stream
      begin
        Misc.consume_stream stream 
        stream.join if stream.respond_to? :join
      rescue Exception
        stream.abort $!
        self._abort
      end
    end
  end

  def soft_grace
    until done? or (Open.exist?(info_file) && info[:status] != :noinfo)
      sleep 1 
    end
    self
  end

  def grace
    until done? or result or error? or aborted? or streaming? or waiting?
      sleep 1 
    end
    self
  end

  def join

    grace if Open.exists?(info_file) 

    if streaming?
      join_stream 
    end

    return self if not Open.exists? info_file

    return self if info[:joined]

    pid = @pid 

    Misc.insist [0.1, 0.2, 0.5, 1] do
      pid ||= info[:pid]
    end

    begin

      if pid.nil? or Process.pid == pid
        dependencies.each{|dep| dep.join }
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

      until (Open.exists?(path) && status == :done) or error? or aborted? or waiting?
        sleep 1
        join_stream if streaming?
      end

      self
    ensure
      begin
        set_info :joined, true 
      rescue
      end if Open.exists?(info_file) && writable?
      @result = nil
    end
  end
end
