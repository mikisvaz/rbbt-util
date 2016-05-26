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
    if Proc === @path
      @path = Path.setup(Misc.sanitize_filename(@path.call))
    else
      @path
    end
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

  def prepare_result(value, description = nil, entity_info = nil)
    res = case 
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
      entity_info ||= begin 
                        entity_info = info.dup
                        entity_info.merge! info[:inputs] if info[:inputs]
                        entity_info
                      end
      entity_info.each do |k,v|
        value.send("#{h}=", v) if annotations.include? k
      end
                        
      value
    else
      entity_info ||= begin 
                        entity_info = info.dup
                        entity_info.merge! info[:inputs] if info[:inputs]
                        entity_info
                      end
      Entity.formats[description].setup(value, entity_info.merge(:format => description))
    end

    if Annotated === res
      dep_hash = nil
      res.annotations.each do |a|
        a = a.to_s
        varname = "@" + a
        next unless res.instance_variable_get(varname).nil? 

        dep_hash ||= begin
                       h = {}
                       rec_dependencies.each{|dep| h[dep.task.name.to_s] ||= dep }
                       h
                     end
        dep = dep_hash[a]
        next if dep.nil?
        res.send(a.to_s+"=", dep.load)
      end 
    end

    res
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
    res = if @result and not @path == @result
            res = @result
          else
            join if not done?
            @path.exists? ? Persist.load_file(@path, @task.result_type) : exec
          end

    if @task.result_description
      entity_info = info.dup
      entity_info.merge! info[:inputs] if info[:inputs]
      res = prepare_result res, @task.result_description, entity_info 
    end

    res
  end

  def self.clean(path)
    info_file = Step.info_file path
    pid_file = Step.pid_file path
    files_dir = Step.files_dir path

    if Open.exists?(path) or Open.exists?(pid_file) or Open.exists?(info_file)

      @result = nil
      @pid = nil

      Misc.insist do
        Open.rm info_file if Open.exists? info_file
        #Open.rm info_file + '.lock' if Open.exists? info_file + '.lock'
        Open.rm path if Open.exists? path
        #Open.rm path + '.lock' if Open.exists? path + '.lock'
        Open.rm_rf files_dir if Open.exists? files_dir
        Open.rm pid_file if Open.exists? pid_file
      end
    end
  end

  def clean
    Log.medium "Cleaning step: #{path}"
    abort if not done? and running?
    Step.clean(path)
    self
  end

  def rec_dependencies

    # A step result with no info_file means that it was manually
    # placed. In that case, do not consider its dependencies
    return [] if not (defined? WorkflowRESTClient and  WorkflowRESTClient::RemoteStep === self) and Open.exists?(self.path.to_s) and not Open.exists? self.info_file

    return [] if dependencies.nil? or dependencies.empty?

    new_dependencies = []
    dependencies.each{|step| 
      r = step.rec_dependencies
      new_dependencies.concat r
      new_dependencies << step
    }
    new_dependencies.uniq
  end

  def recursive_clean
    dependencies.each do |step| 
      step.recursive_clean 
    end
    clean if Open.exists?(self.info_file)
    self
  end

  def step(name)
    @steps ||= {}
    @steps[name] ||= begin
                       deps = rec_dependencies.select{|step| 
                         step.task_name.to_sym == name.to_sym
                       }
                       raise "Dependency step not found: #{ name }" if deps.empty?
                       if (deps & self.dependencies).any?
                         (deps & self.dependencies).first
                       else
                         deps.first
                       end
                     end
  end
end

require 'rbbt/workflow/step/run'
