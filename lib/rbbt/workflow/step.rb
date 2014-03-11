require 'rbbt/persist'
require 'rbbt/persist/tsv'
require 'rbbt/util/log'
require 'rbbt/util/semaphore'
require 'rbbt/workflow/accessor'


class Step
  attr_accessor :path, :task, :inputs, :dependencies, :bindings
  attr_accessor :pid
  attr_accessor :exec

  def initialize(path, task = nil, inputs = nil, dependencies = nil, bindings = nil)
    path = Path.setup(Misc.sanitize_filename(path)) if String === path
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
    @inputs = inputs || []
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
          original_log(status, message)
          relay_step.log([task.name.to_s, status.to_s] * ">", message.nil? ? nil : [task.name.to_s, message] * ">")
        end
      end
    end
    @relay_step = step
    self
  end

  def prepare_result(value, description = nil, info = {})
    case
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

  def exec
    @exec = true if @exec.nil?
    result = @task.exec_in((bindings ? bindings : self), *@inputs)
    prepare_result result, @task.result_description
  end

  def join
    if @pid.nil?
      self
    else
      begin
        Log.debug{"Waiting for pid: #{@pid}"}
        Process.waitpid @pid 
      rescue Errno::ECHILD
        Log.debug{"Process #{ @pid } already finished: #{ path }"}
      end if Misc.pid_exists? @pid
      @pid = nil
    end
    self
  end

  def checks
    rec_dependencies.collect{|dependency| dependency.path }.uniq
  end

  def run(no_load = false)

    result = Persist.persist "Job", @task.result_type, :file => path, :check => checks, :no_load => no_load do
      if Step === Step.log_relay_step and not self == Step.log_relay_step
        relay_log(Step.log_relay_step) unless self.respond_to? :relay_step and self.relay_step
      end
      @exec = false

      Open.rm info_file if Open.exists? info_file

      set_info :pid, Process.pid

      set_info :dependencies, dependencies.collect{|dep| [dep.task.name, dep.name]}
      dependencies.each{|dependency| 
        begin
          dependency.relay_log self
          dependency.clean if not dependency.done? and dependency.error?
          dependency.run true
        rescue Exception
          backtrace = $!.backtrace
          set_info :backtrace, backtrace 
          log(:error, "Exception processing dependency #{dependency.path}")
          log(:error, "#{$!.class}: #{$!.message}")
          log(:error, "backtrace: #{$!.backtrace.first}")
          Log.exception $!
          raise "Exception processing dependency #{dependency.path}"
        end
      }
      
      Log.medium{"#{Log.color :magenta, "Starting task"} #{Log.color :yellow, task.name.to_s || ""} [#{Process.pid}]: #{ path }"}
      set_info :status, :started

      set_info :started, (start_time = Time.now)
      
      set_info :inputs, Misc.remove_long_items(Misc.zip2hash(task.inputs, @inputs)) unless task.inputs.nil?

      res = begin
              exec
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
              log(:error, "backtrace: #{$!.backtrace.first}")
              raise $!
            end

      set_info :status, :done
      set_info :done, (done_time = Time.now)
      set_info :time_elapsed, done_time - start_time
      Log.medium{"#{Log.color :magenta, "Completed task"} #{Log.color :yellow, task.name.to_s || ""} [#{Process.pid}]: #{ path }"}

      res
    end

    if no_load
      self
    else
      prepare_result result, @task.result_description, info
    end
  end

  def fork(semaphore = nil)
    raise "Can not fork: Step is waiting for proces #{@pid} to finish" if not @pid.nil?
    @pid = Process.fork do
      trap(:INT) { raise Aborted.new "INT signal recieved" }
      begin
        RbbtSemaphore.wait_semaphore(semaphore) if semaphore
        FileUtils.mkdir_p File.dirname(path) unless Open.exists? File.dirname(path)
        begin
          run(true)
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
    Process.detach(@pid)
    self
  end

  def abort
    @pid ||= info[:pid]
    if @pid.nil?
      Log.medium "Could not abort #{path}: no pid"
      false
    else
      Log.medium "Aborting #{path}: #{ @pid }"
      begin
        Process.kill("INT", @pid)
        Process.waitpid @pid
      rescue Exception
        Log.debug("Aborted job #{@pid} was not killed: #{$!.message}")
      end
      log(:aborted, "Job aborted by user")
      true
    end
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
    raise "Can not load: Step is waiting for proces #{@pid} to finish" if not done?
    result = Persist.persist "Job", @task.result_type, :file => @path, :check => checks do
      exec
    end
    prepare_result result, @task.result_description, info
  end

  def clean
    if Open.exists?(path) or Open.exists?(info_file)
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
    return [] if self.done? and not Open.exists? self.info_file

    @dependencies.collect{|step| 
      step.rec_dependencies 
    }.flatten.concat  @dependencies
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
    rec_dependencies.select{|step| step.task.name.to_sym == name.to_sym}.first
  end
end
