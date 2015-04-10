require 'rbbt/util/open' 
require 'yaml'

class Step


  INFO_SERIALIAZER = Marshal

  def self.started?
    info_file.exists?
  end
  
  def self.wait_for_jobs(jobs)
    jobs = [jobs] if Step === jobs
    begin
      threads = []
      jobs.each do |j| threads << Thread.new{j.join} end
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
    path.sub(/.*\/#{Regexp.quote task.name.to_s}\/(.*)/, '\1')
  end

  def task_name
    @task_name ||= task.name
  end

  # {{{ INFO

  def info_file
    @info_file ||= Step.info_file(path)
  end

  def info_lock
    @info_lock ||= begin
                     path = Persist.persistence_path(info_file + '.lock', {:dir => Step.lock_dir})
                     Lockfile.new path
                   end
  end

  def info(check_lock = true)
    return {} if info_file.nil? or not Open.exists? info_file
    begin
      Misc.insist do
        begin
          return @info_cache if @info_cache and File.ctime(info_file) < @info_cache_time
        rescue Exception
          raise $!
        end

        begin
          @info_cache = Misc.insist(3, 1.6, info_file) do
            Misc.insist(2, 1, info_file) do
              Misc.insist(3, 0.2, info_file) do
                raise TryAgain, "Info locked" if check_lock and info_lock.locked?
                Open.open(info_file) do |file|
                  INFO_SERIALIAZER.load(file) #|| {}
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

  def set_info(key, value)
    return nil if @exec or info_file.nil?
    value = Annotated.purge value if defined? Annotated
    Open.lock(info_file, :lock => info_lock) do
      i = info(false)
      i[key] = value 
      @info_cache = i
      Misc.sensiblewrite(info_file, INFO_SERIALIAZER.dump(i), :force => true, :lock => false)
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
      Misc.sensiblewrite(info_file, INFO_SERIALIAZER.dump(i), :force => true)
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
      end
    end
  end

  def log_progress(status, options = {}, &block)
    Step.log_progress(status, options, file(:progress), &block)
  end

  def progress_bar(msg, options = {})
    max = options[:max]
    Log::ProgressBar.new max, {:desc => msg, :file => file(:progress)}.merge(options)
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
    self.message Log.uncolor(message)
    Step.log(status, message, path, &block)
  end

  def exception(ex, msg = nil)
    self._abort
    ex_class = ex.class.to_s
    set_info :backtrace, ex.backtrace
    set_info :exception, {:class => ex_class, :message => ex.message, :backtrace => ex.backtrace}
    if msg.nil?
      log :error, "#{ex_class} -- #{ex.message}"
    else
      log :error, "#{msg} -- #{ex.message}"
    end
  end

  def started?
    Open.exists? info_file or Open.exists? path
  end

  def done?
    path and File.exists? path
  end

  def streaming?
    IO === @result or @saved_stream or status == :streaming
  end

  def running?
    return nil if not Open.exists? info_file
    return nil if info[:pid].nil?

    pid = @pid || info[:pid]
    return Misc.pid_exists?(pid) 
  end

  def error?
    status == :error
  end

  def aborted?
    @aborted || status == :aborted
  end

  # {{{ INFO

  def files_dir
    @files_dir ||= Step.files_dir path
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
      if File.exists? dep.info_file
        provenance[dep.path] = dep.provenance if File.exists? dep.path
      else
        provenance[dep.path] = nil
      end
    end
    {:inputs => info[:inputs], :provenance => provenance}
  end

  def provenance_paths
    provenance = {}
    dependencies.each do |dep|
      provenance[dep.path] = dep.provenance_paths if File.exists? dep.path
    end
    provenance
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
    input_defaults = rec_input_defaults(name)
    input_options = rec_input_options(name)
    export = case
             when (synchronous_exports.include?(name.to_sym) or synchronous_exports.include?(name.to_s))
               :synchronous
             when (asynchronous_exports.include?(name.to_sym) or asynchronous_exports.include?(name.to_s))
               :asynchronous
             when (exec_exports.include?(name.to_sym) or exec_exports.include?(name.to_s))
               :exec
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
                              all_deps = deps.select{|dep| String === dep or Symbol === dep or Array === dep}
                              deps.each do |dep| 
                                case dep
                                when Array
                                  dep.first.rec_dependencies(dep.last).each do |d|
                                    if Array === d
                                      all_deps << d
                                    else
                                      all_deps << [dep.first, d]
                                    end
                                  end
                                when String, Symbol
                                  all_deps.concat rec_dependencies(dep.to_sym)
                                when DependencyBlock
                                  all_deps << dep.dependency
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
    dep_inputs = task.dep_inputs rec_dependencies(taskname), self
    task.inputs + dep_inputs.values.flatten
  end

  def rec_input_defaults(taskname)
    [taskname].concat(rec_dependencies(taskname)).inject(IndiferentHash.setup({})){|acc, tn|
      new = (Array === tn ? tn.first.tasks[tn[1].to_sym] : tasks[tn.to_sym]).input_defaults
      acc = new.merge(acc) 
    }.tap{|h| IndiferentHash.setup(h)}
  end

  def rec_input_types(taskname)
    [taskname].concat(rec_dependencies(taskname)).inject({}){|acc, tn|
      new = (Array === tn ? tn.first.tasks[tn[1].to_sym] : tasks[tn.to_sym]).input_types
      acc = new.merge(acc) 
    }.tap{|h| IndiferentHash.setup(h)}
  end

  def rec_input_descriptions(taskname)
    [taskname].concat(rec_dependencies(taskname)).inject({}){|acc, tn|
      new = (Array === tn ? tn.first.tasks[tn[1].to_sym] : tasks[tn.to_sym]).input_descriptions
      acc = new.merge(acc) 
    }.tap{|h| IndiferentHash.setup(h)}
  end

  def rec_input_options(taskname)
    [taskname].concat(rec_dependencies(taskname)).inject({}){|acc, tn|
      new = (Array === tn ? tn.first.tasks[tn[1].to_sym] : tasks[tn.to_sym]).input_options
      acc = new.merge(acc) 
    }.tap{|h| IndiferentHash.setup(h)}
  end

  def real_dependencies(task, jobname, inputs, dependencies)
    real_dependencies = []
    dependencies.each do |dependency|
      real_dependencies << case dependency
      when Array
        workflow, task, options = dependency

        #options = dependency.last if Hash === dependency.last
        _inputs = inputs.dup
        options.each{|i,v|
          case v
          when Symbol
            rec_dependency = (real_dependencies + real_dependencies.collect{|d| d.rec_dependencies}).flatten.compact.uniq.select{|d| d.task.name.to_sym == v }.first
            raise "Dependency for parameter #{i} not found: #{v}" if rec_dependency.nil?
            input_options = workflow.task_info(task)[:input_options][i] || {}
            if input_options[:stream]
              rec_dependency.run(true).grace unless rec_dependency.done? or rec_dependency.running?
              _inputs[i] = rec_dependency
            else
              _inputs[i] = rec_dependency.run
            end
          else
            _inputs[i] = v
          end
        } if options

        res = workflow.job(task, jobname, _inputs)
        res
      when Step
        dependency
      when Symbol
        job(dependency, jobname, inputs)
      when Proc
        dependency.call jobname, inputs, real_dependencies
      end
    end
    real_dependencies.flatten.compact
  end

  TAG = :hash
  def step_path(taskname, jobname, inputs, dependencies, extension = nil)
    #Proc.new{
      raise "Jobname makes an invalid path: #{ jobname }" if jobname =~ /\.\./
      if inputs.any? or dependencies.any?
        tagged_jobname = case TAG
                         when :hash
                           input_str = ""
                           input_str << inputs.collect{|i| Misc.fingerprint(i) } * "," 
                           input_str << ";" << dependencies.collect{|dep| dep.name } * "\n"
                           jobname + '_' << Misc.digest(input_str)
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
    #}
  end

  def id_for(path)
    if workdir.respond_to? :find
      workdir_find = workdir.find 
    else
      workdir_find = workdir
    end
    Misc.path_relative_to workdir_find, path
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
    Misc.path_relative_to(workdir_find, dir).sub(/([^\/]+)\/.*/,'\1')
  end
end
