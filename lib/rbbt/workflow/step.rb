require 'rbbt/persist'
require 'rbbt/persist/tsv'
require 'rbbt/util/log'
require 'rbbt/util/semaphore'
require 'rbbt/workflow/accessor'

class Step
  attr_accessor :clean_name, :path, :task, :inputs, :dependencies, :bindings
  attr_accessor :pid
  attr_accessor :exec
  attr_accessor :result, :mutex, :seen

  class << self
    attr_accessor :lock_dir
    
    def lock_dir
      @lock_dir ||= begin
                      dir = Rbbt.tmp.step_info_locks.find
                      FileUtils.mkdir_p dir unless Open.exists? dir
                      dir
                    end
    end
  end

  def clean_name
    @clean_name ||= begin
                      info[:clean_name] || path.sub(/_[a-z0-9]{32}/, '')
                    end
  end

  def initialize(path, task = nil, inputs = nil, dependencies = nil, bindings = nil, clean_name = nil)
    path = Path.setup(Misc.sanitize_filename(path)) if String === path
    path = path.call if Proc === path

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
    NamedArray.setup @inputs, task.inputs.collect{|s| s.to_s} if task and task.respond_to? :inputs and task.inputs
  end

  def inputs
    if @inputs.nil? and task and task.respond_to? :inputs
      @inputs = info[:inputs].values_at *task.inputs.collect{|name| name.to_s}
    end

    if task.inputs and not NamedArray === @inputs
      NamedArray.setup @inputs, task.inputs 
    end

    @inputs
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
          message Log.uncolor message
          relay_step.log([task.name.to_s, status.to_s] * ">", message.nil? ? nil : message ) unless (relay_step.done? or relay_step.error? or relay_step.aborted?)
        end
      end
    end
    @relay_step = step
    self
  end

  def prepare_result(value, description = nil, info = {})
    case 
    when IO === value
      begin
        res = case @task.result_type
              when :array
                array = []
                while line = value.gets
                  array << line.strip
                end
                array
              when :tsv
                begin
                  TSV.open(value)
                rescue IOError
                  TSV.setup({})
                end
              else
                value.read
              end
        value.join if value.respond_to? :join
        res
      rescue Exception
        value.abort if value.respond_to? :abort
        self.abort
        raise $!
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

  def self.clean(path)
    info_file = Step.info_file path
    files_dir = Step.files_dir path
    if Open.exists?(path) or Open.exists?(info_file)
      begin
        self.abort if self.running?
      rescue Exception
      end

      @result = nil
      @pid = nil

      Misc.insist do
        Open.rm info_file if Open.exists? info_file
        Open.rm info_file + '.lock' if Open.exists? info_file + '.lock'
        Open.rm path if Open.exists? path
        Open.rm path + '.lock' if Open.exists? path + '.lock'
        Open.rm_rf files_dir if Open.exists? files_dir
      end
    end
  end

  def clean
    Step.clean(path)
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
    clean
    rec_dependencies.each do |step| 
      if Open.exists?(step.info_file) 
        step.clean 
      else
      end
    end
    self
  end

  def step(name)
    @steps ||= {}
    @steps[name] ||= begin
                       deps = rec_dependencies.select{|step| 
                         step.task_name.to_sym == name.to_sym
                       }
                       deps.first
                     end

  end
end

require 'rbbt/workflow/step/run'
