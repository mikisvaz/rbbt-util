require 'rbbt/util/open' 
require 'yaml'

module ComputeDependency
  attr_accessor :compute
  def self.setup(dep, value)
    dep.extend ComputeDependency
    dep.compute = value
  end
  
  def canfail?
    compute == :canfail || (Array === compute && compute.include?(:canfail))
  end
end

class Step


  INFO_SERIALIAZER = Marshal

  def self.wait_for_jobs(jobs)
    jobs = [jobs] if Step === jobs
    begin
      threads = []

      threads = jobs.collect do |j| 
        Thread.new do
          begin
            j.join unless j.done?
          rescue Exception
            Log.error "Exception waiting for job: #{Log.color :blue, j.path}"
            raise $!
          end
        end
      end

      threads.each{|t| t.join }
    rescue Exception
      threads.each{|t| t.exit }
      jobs.each do |j| j.abort end
      raise $!
    end
  end

  def self.files_dir(path)
    path.nil? ? nil : path + '.files'
  end

  def self.info_file(path)
    path.nil? ? nil : path + '.info'
  end

  def self.tmp_path(path)
    path = path.find if Path === path
    path = File.expand_path(path)
    dir = File.dirname(path)
    filename = File.basename(path)
    File.join(dir, '.' << filename)
  end

  def self.pid_file(path)
    path.nil? ? nil : path + '.pid'
  end

  def self.step_info(path)
    begin
      Open.open(info_file(path)) do |f|
        INFO_SERIALIAZER.load(f)
      end
    rescue Exception
      Log.exception $!
      {}
    end
  end

  def self.job_name_for_info_file(info_file, extension = nil)
    if extension and not extension.empty?
      info_file.sub(/\.#{extension}\.info$/,'')
    else
      info_file.sub(/\.info$/,'')
    end
  end

  def name
    path.sub(/.*\/#{Regexp.quote task_name.to_s}\/(.*)/, '\1')
  end

  def short_path
    [task_name, name] * "/"
  end

  def task_name
    @task_name ||= task.name
  end

  # {{{ INFO

  def info_file
    @info_file ||= Step.info_file(path)
  end

  def pid_file
    @pid_file ||= Step.pid_file(path)
  end

  def info_lock
    @info_lock = begin
                   path = Persist.persistence_path(info_file + '.lock', {:dir => Step.lock_dir})
                   Lockfile.new path, :refresh => false, :dont_use_lock_id => true
                 end if @info_lock.nil?
    @info_lock
  end

  def info(check_lock = true)
    return {:status => :noinfo} if info_file.nil? or not Open.exists? info_file
    begin
      Misc.insist do
        begin
          return @info_cache if @info_cache and @info_cache_time and File.ctime(info_file) < @info_cache_time
        rescue Exception
          raise $!
        end

        begin
          @info_cache = Misc.insist(3, 1.6, info_file) do
            Misc.insist(2, 1, info_file) do
              Misc.insist(3, 0.2, info_file) do
                raise TryAgain, "Info locked" if check_lock and info_lock.locked?
                info_lock.lock if check_lock and false
                begin
                  Open.open(info_file) do |file|
                    INFO_SERIALIAZER.load(file) #|| {}
                  end
                ensure
                  info_lock.unlock if check_lock and false
                end
              end
            end
          end
          @info_cache_time = Time.now
          @info_cache
        end
      end
    rescue Exception
      Log.debug{"Error loading info file: " + info_file}
      Log.exception $!
      Open.rm info_file
      Misc.sensiblewrite(info_file, INFO_SERIALIAZER.dump({:status => :error, :messages => ["Info file lost"]}))
      raise $!
    end
  end

  def init_info
    return nil if @exec or info_file.nil? or Open.exists?(info_file)
    Open.lock(info_file, :lock => info_lock) do
      i = {:status => :waiting, :pid => Process.pid}
      i[:dependencies] = dependencies.collect{|dep| [dep.task_name, dep.name, dep.path]} if dependencies
      @info_cache = i
      Misc.sensiblewrite(info_file, INFO_SERIALIAZER.dump(i), :force => true, :lock => false)
      @info_cache_time = Time.now
    end
  end

  def set_info(key, value)
    return nil if @exec or info_file.nil?
    value = Annotated.purge value if defined? Annotated
    Open.lock(info_file, :lock => info_lock) do
      i = info(false).dup
      i[key] = value 
      @info_cache = i
      Misc.sensiblewrite(info_file, INFO_SERIALIAZER.dump(i), :force => true, :lock => false)
      #Misc.insist(([0.01,0.1,1] * 3).sort) do
      Misc.insist do
        Open.open(info_file) do |file|
          INFO_SERIALIAZER.load(file) 
        end
      end
      @info_cache_time = Time.now
      value
    end
  end

  def merge_info(hash)
    return nil if @exec or info_file.nil?
    value = Annotated.purge value if defined? Annotated
    Open.lock(info_file, :lock => info_lock) do
      i = info(false)
      i.merge! hash
      @info_cache = i
      Misc.sensiblewrite(info_file, INFO_SERIALIAZER.dump(i), :force => true, :lock => false)
      @info_cache_time = Time.now
      value
    end
  end

  def status
    begin
      info[:status]
    rescue Exception
      Log.error "Exception reading status: #{$!.message}" 
      :error
    end
  end

  def status=(status)
    set_info(:status, status)
  end

  def messages
    if messages = info[:messages]
      messages
    else
      set_info(:messages, []) if self.respond_to?(:set_info)
    end
  end

  def message(message)
    set_info(:messages, (messages || []) << message)
  end

  def self.status_color(status)
    status = status.split(">").last
    case status
    when "starting"
      :yellow
    when "error", "aborted"
      :red
    when "done"
      :green
    else
      :cyan
    end
  end

  def self.log_block(status, message, path, &block)
    start = Time.now
    status = status.to_s
    status_color = self.status_color status

    Log.info do 
      now = Time.now
      str = Log.color :reset
      str << "#{ Log.color status_color, status}"
      str << ": #{ message }" if message
      str << " -- #{Log.color :blue, path.to_s}" if path
      str << " #{Log.color :yellow, Process.pid}"
      str
    end
    res = yield
    eend = Time.now
    Log.info do 
      now = Time.now
      str = "#{ Log.color :cyan, status.to_s } +#{Log.color :green, "%.2f" % (eend - start)}"
      str << " -- #{Log.color :blue, path.to_s}" if path
      str << " #{Log.color :yellow, Process.pid}"
      str
    end
    res
  end

  def self.log_string(status, message, path)
    Log.info do 

      status = status.to_s
      status_color = self.status_color status

      str = Log.color :reset
      str << "#{ Log.color status_color, status}"
      str << ": #{ message }" if message
      str << " -- #{Log.color :blue, path.to_s}" if path
      str << " #{Log.color :yellow, Process.pid}"
      str
    end
  end

  def self.log_progress(status, options = {}, path = nil, &block)
    options = Misc.add_defaults options, :severity => Log::INFO, :file => path
    max = Misc.process_options options, :max
    Log::ProgressBar.with_bar(max, options) do |bar|
      begin
        res = yield bar
        raise KeepBar.new res if IO === res
        res
      rescue
        Log.exception $!
        raise $!
      end
    end
  end

  def log_progress(status, options = {}, &block)
    Step.log_progress(status, options, file(:progress), &block)
  end

  def progress_bar(msg = "Progress", options = nil)
    if Hash === msg and options.nil?
      options = msg
      msg = nil
    end
    options = {} if options.nil?

    max = options[:max]
    Log::ProgressBar.new_bar(max, {:desc => msg, :file => file(:progress)}.merge(options))
  end

  def self.log(status, message, path, &block)
    if block
      if Hash === message
        log_progress(status, message, path, &block)
      else
        log_block(status, message, path, &block)
      end
    else
      log_string(status, message, path)
    end
  end

  def log(status, message = nil, &block)
    self.status = status
    if message
      self.message Log.uncolor(message)
    end
    Step.log(status, message, path, &block)
  end

  def exception(ex, msg = nil)
    ex_class = ex.class.to_s
    set_info :backtrace, ex.backtrace
    set_info :exception, {:class => ex_class, :message => ex.message, :backtrace => ex.backtrace}
    if msg.nil?
      log :error, "#{ex_class} -- #{ex.message}"
    else
      log :error, "#{msg} -- #{ex.message}"
    end
    self._abort
  end

  def get_exception
    if info[:exception].nil?
      return Aborted if aborted?
      return Exception.new(messages.last) if error?
      Exception.new "" 
    else
      ex_class, ex_message, ex_backtrace = info[:exception].values_at :class, :message, :backtrace
      begin
        klass = Kernel.const_get(ex_class)
        ex = klass.new ex_message
        ex.set_backtrace ex_backtrace unless ex_backtrace.nil? or ex_backtrace.empty?
        ex
      rescue
        Log.exception $!
        Exception.new ex_message
      end
    end
  end

  def recoverable_error?
    return true if aborted?
    return false unless error?
    begin
      return true unless info[:exception]
      klass = Kernel.const_get(info[:exception][:class])
      ! (klass <= RbbtException)
    rescue Exception
      true
    end
  end

  def started?
    Open.exists?(path) or (Open.exists?(pid_file) && Open.exists?(info_file))
  end

  def waiting?
    Open.exists?(info_file) and not started?
  end

  def dirty_files
    canfail_paths = self.canfail_paths
    dirty_files = rec_dependencies.reject{|dep|
      (defined?(WorkflowRESTClient) && WorkflowRESTClient::RemoteStep === dep) || 
        ! Open.exists?(dep.info_file) ||
        (dep.path && (Open.exists?(dep.path) || Open.remote?(dep.path))) || 
        ((dep.error? || dep.aborted?) && (! dep.recoverable_error? || canfail_paths.include?(dep.path)))
    }
  end

  def dirty?
    return true if Open.exists?(pid_file) && ! ( Open.exists?(info_file) || done? )
    return false unless done? || status == :done

    status = self.status

    if done? and not (status == :done or status == :ending or status == :producing) and not status == :noinfo
      return true 
    end
    if status == :done and not done?
      return true 
    end

    if dirty_files.any?
      true
    else
      ! self.updated?
    end
  end

  def done?
    path and Open.exists? path
  end

  def streaming?
    (IO === @result) or (not @saved_stream.nil?) or status == :streaming
  end

  def noinfo?
    status == :noinfo
  end

  def running?
    pid = info[:pid]
    return nil if pid.nil?

    return false if done? or error? or aborted? or not started?

    if Misc.pid_exists?(pid) 
      pid
    else
      false
    end
  end

  def stalled?
    started? && ! (done? || error? || aborted?) && ! running?
  end

  def missing?
    status == :done && ! Open.exists?(path)
  end

  def error?
    status == :error
  end

  def nopid?
    pid = info[:pid]
    pid.nil? && ! (status.nil? || status.nil? || status == :aborted || status == :done || status == :error)
  end

  def aborted?
    status == :aborted || (status != :noinfo && nopid?)
  end

  # {{{ INFO

  def files_dir
    @files_dir ||= Step.files_dir path
  end

  def tmp_path
    @tmp_path ||= Step.tmp_path path
  end

  def files
    files = Dir.glob(File.join(files_dir, '**', '*')).reject{|path| File.directory? path}.collect do |path| 
      Misc.path_relative_to(files_dir, path) 
    end
    files
  end

  def file(name)
    Path.setup(File.join(files_dir, name.to_s))
  end

  def save_file(name, content)
    content = case
              when String === content
                content
              when Array === content
                content * "\n"
              when TSV === content
                content.to_s
              when Hash === content
                content.collect{|*p| p * "\t"} * "\n"
              else
                content.to_s
              end
    Open.write(file(name), content)
  end

  def load_file(name, type = nil, options = {})
    if type.nil? and name =~ /.*\.(\w+)$/
      extension = name.match(/.*\.(\w+)$/)[1]
      case extension
      when "tc"
        type = :tc
      when "tsv"
        type = :tsv
      when "list", "ary", "array"
        type = :array
      when "yaml"
        type = :yaml
      when "marshal"
        type = :marshal
      else
        type = :other
      end
    else
      type ||= :other
    end

    case type.to_sym
    when :tc
      Persist.open_tokyocabinet(file(name), false)
    when :tsv
      TSV.open Open.open(file(name)), options
    when :array
      #Open.read(file(name)).split /\n|,\s*/
      Open.read(file(name)).split "\n"
    when :yaml
      YAML.load(Open.open(file(name)))
    when :marshal
      Marshal.load(Open.open(file(name)))
    else
      Open.read(file(name))
    end
  end

  def provenance
    provenance = {}
    dependencies.each do |dep|
      next unless dep.path.exists?
      if File.exist? dep.info_file
        provenance[dep.path] = dep.provenance if File.exist? dep.path
      else
        provenance[dep.path] = nil
      end
    end
    {:inputs => info[:inputs], :provenance => provenance}
  end

  def provenance_paths
    provenance = {}
    dependencies.each do |dep|
      provenance[dep.path] = dep.provenance_paths if File.exist? dep.path
    end
    provenance
  end

  def config(key, *tokens)
    options = tokens.pop if Hash === tokens.last
    default = options[:default] if options

    new_tokens = []
    if workflow
      workflow_name = workflow.to_s
      new_tokens << ("workflow:" << workflow_name)
      new_tokens << ("task:" << workflow_name << "#" << task_name.to_s)
    end
    new_tokens << ("task:" << task_name.to_s)

    value = Rbbt::Config.get(key, tokens + new_tokens)

    value || default
  end
end

module Workflow

  def log(status, message = nil, &block)
    Step.log(status, message, nil, &block)
  end

  def task_info(name)
    name = name.to_sym
    task = tasks[name]
    raise "No '#{name}' task in '#{self.to_s}' Workflow" if task.nil?
    description = task.description
    result_description = task.result_description
    result_type = task.result_type
    inputs = rec_inputs(name).uniq
    input_types = rec_input_types(name)
    input_descriptions = rec_input_descriptions(name)
    input_use = rec_input_use(name)
    input_defaults = rec_input_defaults(name)
    input_options = rec_input_options(name)
    export = case
             when (synchronous_exports.include?(name.to_sym) or synchronous_exports.include?(name.to_s))
               :synchronous
             when (asynchronous_exports.include?(name.to_sym) or asynchronous_exports.include?(name.to_s))
               :asynchronous
             when (exec_exports.include?(name.to_sym) or exec_exports.include?(name.to_s))
               :exec
             when (stream_exports.include?(name.to_sym) or stream_exports.include?(name.to_s))
               :stream
             else
               :none
             end


    dependencies = task_dependencies[name].select{|dep| String === dep or Symbol === dep}
    { :id => File.join(self.to_s, name.to_s),
      :description => description,
      :export => export,
      :inputs => inputs,
      :input_types => input_types,
      :input_descriptions => input_descriptions,
      :input_defaults => input_defaults,
      :input_options => input_options,
      :input_use => input_use,
      :result_type => result_type,
      :result_description => result_description,
      :dependencies => dependencies
    }
  end

  def rec_dependencies(taskname)
    @rec_dependencies ||= {}
    @rec_dependencies[taskname] ||= begin
                            if task_dependencies.include? taskname

                              deps = task_dependencies[taskname]

                              #all_deps = deps.select{|dep| String === dep or Symbol === dep or Array === dep}

                              all_deps = []
                              deps.each do |dep| 
                                if DependencyBlock === dep
                                  all_deps << dep.dependency if dep.dependency
                                else
                                  all_deps << dep unless Proc === dep
                                end

                                begin
                                  case dep
                                  when Array
                                    wf, t, o = dep

                                    wf.rec_dependencies(t).each do |d|
                                      if Array === d
                                        new = d.dup
                                      else
                                        new = [dep.first, d]
                                      end

                                      if Hash === o and not o.empty? 
                                        if Hash === new.last
                                          hash = new.last.dup
                                          o.each{|k,v| hash[k] ||= v}
                                          new[new.length-1] = hash
                                        else
                                          new.push o.dup
                                        end
                                      end

                                      all_deps << new
                                    end

                                  when String, Symbol
                                    rec_deps = rec_dependencies(dep.to_sym)
                                    all_deps.concat rec_deps
                                  when DependencyBlock
                                    dep = dep.dependency
                                    raise TryAgain
                                  end
                                rescue TryAgain
                                  retry
                                end
                              end
                              all_deps.uniq
                            else
                              []
                            end
                          end
  end

  def task_from_dep(dep)
    task = case dep
           when Array
             dep.first.tasks[dep[1]] 
           when String
             tasks[dep.to_sym]
           when Symbol
             tasks[dep.to_sym]
           end
    raise "Unknown dependency: #{Misc.fingerprint dep}" if task.nil?
    task
  end

  #def rec_inputs(taskname)
  #  [taskname].concat(rec_dependencies(taskname)).inject([]){|acc, tn| acc.concat(task_from_dep(tn).inputs) }.uniq
  #end

  def rec_inputs(taskname)
    task = task_from_dep(taskname)
    deps = rec_dependencies(taskname)
    dep_inputs = task.dep_inputs deps, self
    task.inputs + dep_inputs.values.flatten
  end

  def rec_input_defaults(taskname)
    rec_inputs = rec_inputs(taskname)
    [taskname].concat(rec_dependencies(taskname)).inject(IndiferentHash.setup({})){|acc, tn|
      if Array === tn and tn[0] and tn[1]
        new = tn.first.tasks[tn[1].to_sym].input_defaults
      elsif Symbol === tn
        new = tasks[tn.to_sym].input_defaults
      else
        next acc
      end
      acc = new.merge(acc) 
      acc.delete_if{|input,defaults| not rec_inputs.include? input}
      acc
    }.tap{|h| IndiferentHash.setup(h)}
  end

  def rec_input_types(taskname)
    rec_inputs = rec_inputs(taskname)
    [taskname].concat(rec_dependencies(taskname)).inject({}){|acc, tn|
      if Array === tn and tn[0] and tn[1]
        new = tn.first.tasks[tn[1].to_sym].input_types
      elsif Symbol === tn
        new = tasks[tn.to_sym].input_types
      else
        next acc
      end
      acc = new.merge(acc) 
      acc.delete_if{|input,defaults| not rec_inputs.include? input}
      acc
    }.tap{|h| IndiferentHash.setup(h)}
  end
  
  def rec_input_use(taskname)
    task = task_from_dep(taskname)
    deps = rec_dependencies(taskname)
    inputs = {}
    task.inputs.each do |input|
      name = task.name
      workflow = (task.workflow || self).to_s

      inputs[input] ||= {}
      inputs[input][workflow] ||= []
      inputs[input][workflow]  << name
    end

    dep_inputs = Task.dep_inputs deps, self

    dep_inputs.each do |dep,is|
      name = dep.name
      workflow = dep.workflow

      is.each do |input|
        inputs[input] ||= {}
        inputs[input][workflow] ||= []
        inputs[input][workflow]  << name
      end
    end

    inputs
  end

  def rec_input_descriptions(taskname)
    rec_inputs = rec_inputs(taskname)
    [taskname].concat(rec_dependencies(taskname)).inject({}){|acc, tn|
      if Array === tn and tn[0] and tn[1]
        new = tn.first.tasks[tn[1].to_sym].input_descriptions
      elsif Symbol === tn
        new = tasks[tn.to_sym].input_descriptions
      else
        next acc
      end
      acc = new.merge(acc) 
      acc.delete_if{|input,defaults| not rec_inputs.include? input}
      acc
    }.tap{|h| IndiferentHash.setup(h)}
  end

  def rec_input_options(taskname)
    rec_inputs = rec_inputs(taskname)
    [taskname].concat(rec_dependencies(taskname)).inject({}){|acc, tn|
      if Array === tn and tn[0] and tn[1]
        new = tn.first.tasks[tn[1].to_sym].input_options
      elsif Symbol === tn
        new = tasks[tn.to_sym].input_options
      else
        next acc
      end
      acc = new.merge(acc) 
      acc = acc.delete_if{|input,defaults| not rec_inputs.include? input}
      acc
    }.tap{|h| IndiferentHash.setup(h)}
  end

  def assign_dep_inputs(_inputs, options, all_d, task_info)
    options.each{|i,v|
      next if i == :compute or i == "compute"
      case v
      when :compute
        compute = v
      when Symbol
        rec_dependency = all_d.flatten.select{|d| d.task_name.to_sym == v }.first

        if rec_dependency.nil?
          if _inputs.include? v
            _inputs[i] = _inputs.delete(v)
          else
            _inputs[i] = v unless _inputs.include? i
          end
        else
          input_options = task_info[:input_options][i] || {}
          if input_options[:stream] or true
            #rec_dependency.run(true).grace unless rec_dependency.done? or rec_dependency.running?
            _inputs[i] = rec_dependency
          else
            rec_dependency.abort if rec_dependency.streaming? and not rec_dependency.running?
            rec_dependency.clean if rec_dependency.error? or rec_dependency.aborted?
            if rec_dependency.streaming? and rec_dependency.running?
              _inputs[i] = rec_dependency.join.load
            else
              rec_dependency.run(true)
              rec_dependency.join
              _inputs[i] = rec_dependency.load
            end
          end
        end
      else
        _inputs[i] = v
      end
    } if options

    _inputs
  end

  def real_dependencies(task, orig_jobname, inputs, dependencies)
    real_dependencies = []
    path_deps = {}

    override_dependencies = IndiferentHash.setup({})

    inputs.each do |key,value|
      if String === key && m = key.match(/(.*)#(.*)/)
        workflow, task = m.values_at 1, 2
        workflow = self.to_s if workflow.empty?
        override_dependencies[workflow] ||= IndiferentHash.setup({})
        override_dependencies[workflow][task] = value
      end
    end

    dependencies.each do |dependency|
      _inputs = IndiferentHash.setup(inputs.dup)
      jobname = orig_jobname
      jobname = _inputs[:jobname] if _inputs.include? :jobname

      real_dep = case dependency
                 when Array
                   workflow, dep_task, options = dependency

                   if override_dependencies[workflow.to_s] && value = override_dependencies[workflow.to_s][dep_task]
                     d_ = Step === value ? value : Workflow.load_step(value)
                     d_.task = workflow.tasks[dep_task]
                     d_.workflow = workflow
                     d_.overriden = true
                     d_
                   else

                     compute = options[:compute] if options

                     all_d = (real_dependencies + real_dependencies.flatten.collect{|d| d.rec_dependencies} ).flatten.compact.uniq

                     _inputs = assign_dep_inputs(_inputs, options, all_d, workflow.task_info(dep_task))
                     jobname = _inputs[:jobname] if _inputs.include? :jobname

                     job = workflow.job(dep_task, jobname, _inputs)
                     ComputeDependency.setup(job, compute) if compute
                     job
                   end
                 when Step
                   dependency
                 when Symbol
                   if override_dependencies[self.to_s] && value = override_dependencies[self.to_s][dependency]
                     d_ = Step === value ? value : Workflow.load_step(value)
                     d_.task = self.tasks[dependency]
                     d_.workflow = self
                     d_.overriden = true
                     d_
                   else
                     job(dependency, jobname, _inputs)
                   end
                 when Proc
                   if DependencyBlock === dependency
                     orig_dep = dependency.dependency 
                     wf, task_name, options = orig_dep

                     options = {} if options.nil?
                     compute = options[:compute]

                     options = IndiferentHash.setup(options.dup)
                     dep = dependency.call jobname, options.merge(_inputs), real_dependencies

                     dep = [dep] unless Array === dep

                     new_=[]
                     dep.each{|d| 
                       next if d.nil?
                       if Hash === d
                         d[:workflow] ||= wf 
                         d[:task] ||= task_name
                         if override_dependencies[d[:workflow].to_s] && value = override_dependencies[d[:workflow].to_s][d[:task]]
                           d = (Step === value ? value : Workflow.load_step(value))
                           d.task = d[:workflow].tasks[d[:task]]
                           d.workflow = self
                           d.overriden = true
                           d
                         else
                           task_info = d[:workflow].task_info(d[:task])

                           inputs = assign_dep_inputs({}, options.merge(d[:inputs] || {}), real_dependencies, task_info) 
                           d = d[:workflow].job(d[:task], d[:jobname], inputs) 
                         end
                       end
                       ComputeDependency.setup(d, compute) if compute
                       new_ << d
                     }
                     dep = new_
                   else
                     _inputs = IndiferentHash.setup(_inputs.dup)
                     dep = dependency.call jobname, _inputs, real_dependencies
                     if Hash === dep
                       dep[:workflow] ||= wf  || self
                       if override_dependencies[d[:workflow].to_s] && value = override_dependencies[d[:workflow].to_s][d[:task]]
                         dep = (Step === value ? value : Workflow.load_step(value))
                         dep.task = d[:workflow].tasks[d[:task]]
                         dep.workflow = self
                         dep.overriden = true
                       else
                         task_info = (dep[:task] && dep[:workflow]) ? dep[:workflow].task_info(dep[:task]) : nil
                         inputs = assign_dep_inputs({}, dep[:inputs], real_dependencies, task_info)
                         dep = dep[:workflow].job(dep[:task], dep[:jobname], inputs)
                       end
                     end
                   end

                   dep
                 else
                   raise "Dependency for #{task.name} not understood: #{Misc.fingerprint dependency}"
                 end

      real_dependencies << real_dep
    end
    real_dependencies.flatten.compact
  end

  TAG = ENV["RBBT_INPUT_JOBNAME"] == "true" ? :inputs : :hash
  def step_path(taskname, jobname, inputs, dependencies, extension = nil)
    raise "Jobname makes an invalid path: #{ jobname }" if jobname.include? '..'
    if inputs.length > 0 or dependencies.any?
      tagged_jobname = case TAG
                       when :hash
                         clean_inputs = Annotated.purge(inputs)
                         clean_inputs = clean_inputs.collect{|i| Symbol === i ? i.to_s : i }
                         key_obj = {:inputs => clean_inputs, :dependencies => dependencies}
                         key_str = Misc.obj2str(key_obj)
                         hash_str = Misc.digest(key_str)
                         Log.debug "Hash for '#{[taskname, jobname] * "/"}' #{hash_str} for #{key_str}"
                         jobname + '_' << hash_str
                       when :inputs
                         all_inputs = {}
                         inputs.zip(self.task_info(taskname)[:inputs]) do |i,f|
                           all_inputs[f] = i
                         end
                         dependencies.each do |dep|
                           ri = dep.recursive_inputs
                           ri.zip(ri.fields).each do |i,f|
                             all_inputs[f] = i
                           end
                         end

                         all_inputs.any? ? jobname + '_' << Misc.obj2str(all_inputs) : jobname
                       else
                         jobname
                       end
    else
      tagged_jobname = jobname
    end

    if extension and not extension.empty?
      tagged_jobname = tagged_jobname + ('.' << extension.to_s)
    end

    workdir[taskname][tagged_jobname].find
  end

  def id_for(path)
    if workdir.respond_to? :find
      workdir_find = workdir.find 
    else
      workdir_find = workdir
    end
    Misc.path_relative_to workdir_find, path
  end

  def self.workflow_for(path)
    begin
      Kernel.const_get File.dirname(File.dirname(path))
    rescue
      nil
    end
  end

  def task_for(path)
    if workdir.respond_to? :find
      workdir_find = workdir.find 
    else
      workdir_find = workdir
    end
 
    workdir_find = File.expand_path(workdir_find)
    path = File.expand_path(path)
    dir = File.dirname(path)
    begin
      Misc.path_relative_to(workdir_find, dir).sub(/([^\/]+)\/.*/,'\1')
    rescue
      nil
    end
  end

  def task_exports
    [exec_exports, synchronous_exports, asynchronous_exports, stream_exports].compact.flatten.uniq
  end

end
