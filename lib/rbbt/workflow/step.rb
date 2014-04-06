require 'rbbt/persist'
require 'rbbt/persist/tsv'
require 'rbbt/util/log'
require 'rbbt/util/semaphore'
require 'rbbt/workflow/accessor'


class Step
  attr_accessor :path, :task, :inputs, :dependencies, :bindings
  attr_accessor :pid
  attr_accessor :exec
  attr_accessor :result, :mutex

  def initialize(path, task = nil, inputs = nil, dependencies = nil, bindings = nil)
    path = Path.setup(Misc.sanitize_filename(path)) if String === path
    pat = path.call if Proc === path
    @path = path
    @task = task
    @bindings = bindings
    @dependencies = case
                    when dependencies.nil? 
                      []
                    when Array === dependencies
                      dependencies
                    else
                      [dependencies]
                    end
    @mutex = Mutex.new
    @info_mutex = Mutex.new
    @inputs = inputs || []
  end

  def task_name
    @task.name
  end

  def path
    @path = Misc.sanitize_filename(Path.setup(@path.call)) if Proc === @path
    @path
  end

  class << self
    attr_accessor :log_relay_step
  end

  def relay_log(step)
    return self unless Task === self.task and not self.task.name.nil?
    if not self.respond_to? :original_log
      class << self
        attr_accessor :relay_step
        alias original_log log 
        def log(status, message = nil)
          self.status = status
          message message
          relay_step.log([task.name.to_s, status.to_s] * ">", message.nil? ? nil : message )
        end
      end
    end
    @relay_step = step
    self
  end

  def prepare_result(value, description = nil, info = {})
    #info = self.info
    case 
    when IO === value
      begin
        case @task.result_type
        when :array
          array = []
          while line = value.gets
            array << line
          end
          array
        when :tsv
          TSV.open(value)
        else
          value.read
        end
      rescue Exception
        value.abort if value.respond_to? :abort
      ensure
        value.close unless value.closed?
        value.join if value.respond_to? :join
      end
    when (not defined? Entity or description.nil? or not Entity.formats.include? description)
      value
    when (Annotated === value and info.empty?)
      value
    when Annotated === value
      annotations = value.annotations
      info.each do |k,v|
        value.send("#{h}=", v) if annotations.include? k
      end
      value
    else
      Entity.formats[description].setup(value, info.merge(:format => description))
    end
  end

  def get_stream
    @mutex.synchronize do
      begin
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
    @result = _exec
    @result = @result.stream if TSV::Dumper === @result
    no_load ? @result : prepare_result(@result, @task.result_description)
  end

  def join
    stream = get_stream
    begin
      Misc.consume_stream stream if stream
    rescue
      stream.abort if stream.respond_to? :abort
      raise $!
    ensure
      stream.join if stream.respond_to? :join and not stream.joined?
    end


    if @pid.nil?
      dependencies.each{|dep| dep.join }
      self
    else
      begin
        Log.debug{"Waiting for pid: #{@pid}"}
        Process.waitpid @pid 
      rescue Errno::ECHILD
        Log.debug{"Process #{ @pid } already finished: #{ path }"}
      end if Misc.pid_exists? @pid
      @pid = nil
      dependencies.each{|dep| dep.join }
      self
    end
  end

  def checks
    rec_dependencies.collect{|dependency| dependency.path }.uniq
  end

  def run(no_load = false)

    @mutex.synchronize do
      result = Persist.persist "Job", @task.result_type, :file => path, :check => checks, :no_load => no_load ? :stream : false do
        if Step === Step.log_relay_step and not self == Step.log_relay_step
          relay_log(Step.log_relay_step) unless self.respond_to? :relay_step and self.relay_step
        end
        @exec = false

        Open.rm info_file if Open.exists? info_file

        set_info :pid, Process.pid
        set_info :issued, Time.now

        log(:preparing, "Preparing job: #{Misc.fingerprint dependencies}")
        set_info :dependencies, dependencies.collect{|dep| [dep.task_name, dep.name]}
        seen_deps = []
        dependencies.uniq.each{|dependency| 
          Log.info "#{Log.color :magenta, "Checking dependency"} #{Log.color :yellow, task.name.to_s || ""} => #{Log.color :yellow, dependency.task_name.to_s || ""}"
          begin
            next if seen_deps.include? dependency.path
            dependency.relay_log self
            dependency.clean if not dependency.done? and dependency.error?
            dependency.clean if dependency.streaming? and not dependency.running?
            dependency.run true unless dependency.result 
            seen_deps.concat dependency.rec_dependencies.collect{|d| d.path} 
          rescue Exception
            backtrace = $!.backtrace
            set_info :backtrace, backtrace 
            log(:error, "Exception processing dependency #{Log.color :yellow, dependency.task.name.to_s} -- #{$!.class}: #{$!.message}")
            raise $!
          end
        }

        set_info :inputs, Misc.remove_long_items(Misc.zip2hash(task.inputs, @inputs)) unless task.inputs.nil?

        set_info :started, (start_time = Time.now)
        log :started, "#{Log.color :green, "Starting task"} #{Log.color :yellow, task.name.to_s || ""} [#{Process.pid}]"

        begin
          result = _exec
        rescue Aborted
          log(:error, "Aborted")

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

          raise $!
        rescue Exception
          backtrace = $!.backtrace

          # HACK: This fixes an strange behaviour in 1.9.3 where some
          # backtrace strings are coded in ASCII-8BIT
          backtrace.each{|l| l.force_encoding("UTF-8")} if String.instance_methods.include? :force_encoding

          set_info :backtrace, backtrace 
          log(:error, "#{$!.class}: #{$!.message}")
          raise $!
        end

        case result
        when IO
          log :streaming, "#{Log.color :magenta, "Streaming task result IO"} #{Log.color :yellow, task.name.to_s || ""} [#{Process.pid}]"
          ConcurrentStream.setup result do
            begin
              set_info :done, (done_time = Time.now)
              set_info :time_elapsed, (time_elapsed = done_time - start_time)
              log :done, "#{Log.color :red, "Completed task"} #{Log.color :yellow, task.name.to_s || ""} [#{Process.pid}] +#{time_elapsed.to_i} -- #{path}"
            rescue
              Log.exception $!
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
  end

  def fork(semaphore = nil)
    raise "Can not fork: Step is waiting for proces #{@pid} to finish" if not @pid.nil?
    @pid = Process.fork do
      begin
        RbbtSemaphore.wait_semaphore(semaphore) if semaphore
        FileUtils.mkdir_p File.dirname(path) unless Open.exists? File.dirname(path)
        begin
          res = run(true)
          io = get_stream
          if IO === io
            Misc.consume_stream(io)
            io.close unless io.closed?
            io.join if io.respond_to? :join and not io.joined?
          end
        rescue Aborted
          Log.debug{"Forked process aborted: #{path}"}
          log :aborted, "Aborted"
          raise $!
        rescue Exception
          Log.debug("Exception '#{$!.message}' caught on forked process: #{path}")
          raise $!
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

  def abort
    @pid ||= info[:pid]

    return true unless info[:forked]

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
      log(:aborted, "Job aborted by user")
      true
    end
    log(:aborted, "Job aborted by user")
  end

  def child(&block)
    child_pid = Process.fork &block
    children_pids = info[:children_pids]
    if children_pids.nil?
      children_pids = [child_pid]
    else
      children_pids << child_pid
    end
    #Process.detach(child_pid)
    set_info :children_pids, children_pids
    child_pid
  end

  def load
    return prepare_result @result, @task.result_description if @result and not @path == @result
    join if not done?
    return Persist.load_file(@path, @task.result_type) if @path.exists?
    exec
  end

  def clean
    if Open.exists?(path) or Open.exists?(info_file)
      begin
        self.abort if self.running?
      rescue Exception
      end

      @result = nil
      @pid = nil

      begin
        Open.rm info_file if Open.exists? info_file
        Open.rm info_file + '.lock' if Open.exists? info_file + '.lock'
        Open.rm path if Open.exists? path
        Open.rm path + '.lock' if Open.exists? path + '.lock'
        Open.rm_rf files_dir if Open.exists? files_dir
      end
    end
    self
  end

  def rec_dependencies

    # A step result with no info_file means that it was manually
    # placed. In that case, do not consider its dependencies
    return [] if Open.exists?(self.path.to_s) and not Open.exists? self.info_file

    return [] if dependencies.nil? or dependencies.empty?
    new_dependencies = dependencies.collect{|step| 
      step.rec_dependencies 
    }.flatten.uniq.compact

    dependencies = self.dependencies ? self.dependencies + new_dependencies : new_dependencies
    dependencies.flatten!
    dependencies.uniq!
    dependencies
  end

  def recursive_clean
    rec_dependencies.each do |step| 
      if File.exists?(step.info_file) 
        step.clean 
      end
    end
    clean
  end

  def step(name)
    @steps ||= {}
    @steps[name] ||= rec_dependencies.select do |step| 
      step.task_name.to_sym == name.to_sym
    end.first
  end
end
