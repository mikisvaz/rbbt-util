require 'rbbt/persist'
require 'rbbt/persist/tsv'
require 'rbbt/util/log'
require 'rbbt/util/semaphore'
require 'rbbt/workflow/step/accessor'
require 'rbbt/workflow/step/produce'
require 'rbbt/workflow/step/status'
require 'rbbt/workflow/step/info'
require 'rbbt/workflow/step/save_load_inputs'

class Step
  attr_accessor :clean_name, :path, :task, :workflow, :inputs, :dependencies, :bindings
  attr_accessor :task_name, :overriden
  attr_accessor :pid
  attr_accessor :exec
  attr_accessor :relocated
  attr_accessor :result, :mutex, :seen
  attr_accessor :real_inputs, :original_task_name, :original_workflow

  RBBT_DEBUG_CLEAN = ENV["RBBT_DEBUG_CLEAN"] == 'true'

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
    @inputs = inputs 
    NamedArray.setup @inputs, task.inputs.collect{|s| s.to_s} if task and task.respond_to? :inputs and task.inputs
    if Open.exists?(info_file) and (info[:path] != path)
      @relocated = true
    end
  end

  def workflow
    @workflow || info[:workflow]
  end



  def inputs
    return @inputs if NamedArray === @inputs

    load_inputs_from_info if @inputs.nil? 

    NamedArray.setup(@inputs, task.inputs) if task && task.inputs && !(NamedArray === @inputs)

    @inputs || []
  end

  def copy_files_dir
    if File.symlink?(self.files_dir)
      begin
        realpath = Open.realpath(self.files_dir)
        Open.rm self.files_dir
        Open.cp realpath, self.files_dir
      rescue
        Log.warn "Copy files_dir for #{self.workflow_short_path}: " + $!.message
      end
    end
  end

  def recursive_inputs
    if NamedArray === inputs
      i = {}
      inputs.zip(inputs.fields).each do |v,f|
        i[f] = v
      end
    else
      i = {}
    end
    rec_dependencies.each do |dep|
      next unless NamedArray === dep.inputs

      dep.inputs.zip(dep.inputs.fields).each do |v,f|
        if i.include?(f) && i[f] != v
          next
          #Log.debug "Variable '#{f}' reused with different values: #{[Misc.fingerprint(i[f]), Misc.fingerprint(v)] * " <-> "}"
        else 
          i[f] = v
        end
      end

      dep.archived_inputs.each do |k,v|
        i[k] = v unless i.include? k
      end
    end

    self.archived_inputs.each do |k,v|
      i[k] = v unless i.include? k
    end

    #dependencies.each do |dep|
    #  di = dep.recursive_inputs
    #  next unless NamedArray === di
    #  di.fields.zip(di).each do |k,v|
    #    i[k] = v unless i.include? k
    #  end
    #end
    
    v = i.values
    NamedArray.setup v, i.keys 
    v
  end

  def task_name
    @task_name ||= begin
                     if @task.nil?
                        @path.split("/")[-2]
                     else
                       @task.name
                     end
                   end
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

  def result_type
    @result_type ||= if @task.nil?
                       info[:result_type] || :binary
                     else
                       @task.result_type || info[:result_type] || :string
                     end
  end

  def result_type=(type)
    @result_type = type
  end

  def result_description
    @result_description ||= if @task.nil?
                       info[:result_description]
                     else
                       @task.result_description
                     end
  end

  def prepare_result(value, description = nil, entity_info = nil)
    res = case 
    when IO === value
      begin
        res = case result_type
              when :array
                array = []
                while line = value.gets
                  array << line.chomp
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
                       rec_dependencies.each{|dep| h[dep.task_name.to_s] ||= dep }
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
    set_info :children_pids, children_pids
    child_pid
  end

  def cmd(*args)
    all_args = *args

    all_args << {} unless Hash === all_args.last

    level = all_args.last[:log] || 0
    level = 0 if TrueClass === level
    level = 10 if FalseClass === level
    level = level.to_i

    all_args.last[:log] = true
    all_args.last[:pipe] = true

    io = CMD.cmd(*all_args)
    child_pid = io.pids.first

    children_pids = info[:children_pids]
    if children_pids.nil?
      children_pids = [child_pid]
    else
      children_pids << child_pid
    end
    set_info :children_pids, children_pids

    while c = io.getc
      STDERR << c if Log.severity <= level
      if c == "\n"
        if pid
          Log.logn "STDOUT [#{pid}]: ", level
        else
          Log.logn "STDOUT: ", level
        end
      end
    end 

    io.join

    nil
  end


  def load
    res = begin
            @result = nil if IO === @result && @result.closed?
            if @result && @path != @result &&  ! StreamArray === @result
              res = @result
            else
              join if not done?
              res = @path.exists? ? Persist.load_file(@path, result_type) : exec
            end

            if result_description
              entity_info = info.dup
              entity_info.merge! info[:inputs] if info[:inputs]
              res = prepare_result res, result_description, entity_info 
            end

            res
          rescue IOError
            if @result
              @result = nil
              retry
            end
            raise $!
          ensure
            @result = nil if IO === @result
          end

    res
  end

  def update
    if dirty?
      dependencies.collect{|d| d.update } if dependencies
      clean
    end
  end

  def writable?
    Open.writable?(self.path) && Open.writable?(self.info_file)
  end

  def recursive_clean
    dependencies.each do |step| 
      step.recursive_clean 
    end if dependencies
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
                         (deps & self.dependencies).last
                       else
                         deps.last
                       end
                     end
  end
end

require 'rbbt/workflow/step/run'
