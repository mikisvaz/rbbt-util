class Step

  INFO_SERIALIZER = begin
                       if ENV["RBBT_INFO_SERIALIZER"]
                         Kernel.const_get ENV["RBBT_INFO_SERIALIZER"]
                       else
                         Marshal
                       end
                     end

  def self.serialize_info(info)
    info = info.clean_version if IndiferentHash === info
    INFO_SERIALIZER.dump(info)
  end

  def self.load_serialized_info(io)
    IndiferentHash.setup(INFO_SERIALIZER.load(io))
  end


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

  def self.md5_file(path)
    path.nil? ? nil : path + '.md5'
  end

  def self.pid_file(path)
    path.nil? ? nil : path + '.pid'
  end

  def self.step_info(path)
    begin
      Open.open(info_file(path), :mode => 'rb') do |f|
        self.load_serialized_info(f)
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

  def self.save_inputs(inputs, input_types, dir)
    inputs.each do |name,value|
      type = input_types[name]
      type = type.to_s if type
      path = File.join(dir, name.to_s)

      Log.debug "Saving job input #{name} (#{type}) into #{path}"
      case
      when Step === value
        Open.ln_s(value.path, path)
      when type.to_s == "file"
        if String === value && File.exists?(value)
          Open.ln_s(value, path)
        else
          value = "#{value}" if Path === value
          Open.write(path + '.yaml', value.to_yaml)
        end
      when Array === value
        Open.write(path, value.collect{|v| Step === v ? v.path : v.to_s} * "\n")
      when IO === value
        if value.filename && String === value.filename && File.exists?(value.filename)
          Open.ln_s(value.filename, path)
        else
          Open.write(path, value)
        end
      else
        Open.write(path, value.to_s)
      end
    end.any?
  end

  def self.save_job_inputs(job, dir, options = nil)
    options = IndiferentHash.setup options.dup if options

    task_name = Symbol === job.overriden ? job.overriden : job.task_name
    workflow = job.workflow
    workflow = Kernel.const_get workflow if String === workflow
    if workflow
      task_info = IndiferentHash.setup(workflow.task_info(task_name))
      input_types = IndiferentHash.setup(task_info[:input_types])
      task_inputs = IndiferentHash.setup(task_info[:inputs])
      input_defaults = IndiferentHash.setup(task_info[:input_defaults])
    else
      task_info = IndiferentHash.setup({})
      input_types = IndiferentHash.setup({})
      task_inputs = IndiferentHash.setup({})
      input_defaults = IndiferentHash.setup({})
    end

    inputs = IndiferentHash.setup({})
    real_inputs = job.real_inputs || job.info[:real_inputs]
    job.recursive_inputs.zip(job.recursive_inputs.fields).each do |value,name|
      next unless task_inputs.include? name.to_sym
      next unless real_inputs.include? name.to_sym
      next if options && ! options.include?(name)
      next if value.nil?
      next if input_defaults[name] == value
      inputs[name] = value
    end

    if options && options.include?('override_dependencies')
      inputs.merge!(:override_dependencies => open[:override_dependencies])
      input_types = IndiferentHash.setup(input_types.merge(:override_dependencies => :array))
    end
    save_inputs(inputs, input_types, dir)

    inputs.keys
  end

  def name
    @name ||= path.sub(/.*\/#{Regexp.quote task_name.to_s}\/(.*)/, '\1')
  end


  def short_path
    [task_name, name] * "/"
  end

  def workflow_short_path
    return short_path unless workflow
    workflow.to_s + "#" + short_path
  end

  def task_name
    @task_name ||= task.name
  end

  def task_signature
    [workflow.to_s, task_name] * "#"
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
                   #Lockfile.new path, :refresh => false, :dont_use_lock_id => true
                   Lockfile.new path
                 end if @info_lock.nil?
                 @info_lock
  end

  def status_lock
    return @mutex
    #@status_lock = begin
    #               path = Persist.persistence_path(info_file + '.status.lock', {:dir => Step.lock_dir})
    #               Lockfile.new path, :refresh => false, :dont_use_lock_id => true
    #             end if @status_lock.nil?
    #@status_lock
  end

  def info(check_lock = true)
    return {:status => :noinfo} if info_file.nil? or not Open.exists? info_file
    begin
      Misc.insist do
        begin
          return @info_cache if @info_cache and @info_cache_time and Open.ctime(info_file) < @info_cache_time 
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
                  Open.open(info_file, :mode => 'rb') do |file|
                    Step.load_serialized_info(file)
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
      Misc.sensiblewrite(info_file, Step.serialize_info({:status => :error, :messages => ["Info file lost"]}))
      raise $!
    end
  end

  def init_info(force = false)
    return nil if @exec || info_file.nil? || (Open.exists?(info_file) && ! force)
    Open.lock(info_file, :lock => info_lock) do
      i = {:status => :waiting, :pid => Process.pid, :path => path, :real_inputs => real_inputs}
      i[:dependencies] = dependencies.collect{|dep| [dep.task_name, dep.name, dep.path]} if dependencies
      Misc.sensiblewrite(info_file, Step.serialize_info(i), :force => true, :lock => false)
      @info_cache = IndiferentHash.setup(i)
      @info_cache_time = Time.now
    end
  end

  def set_info(key, value)
    return nil if @exec or info_file.nil?
    return nil if ! writable?
    value = Annotated.purge value if defined? Annotated
    Open.lock(info_file, :lock => info_lock) do
      i = info(false).dup
      i[key] = value 
      dump = Step.serialize_info(i)
      @info_cache = IndiferentHash.setup(i)
      Misc.sensiblewrite(info_file, dump, :force => true, :lock => false) if Open.exists?(info_file)
      @info_cache_time = Time.now
      value
    end
  end

  def merge_info(hash)
    return nil if @exec or info_file.nil?
    return nil if ! writable?
    value = Annotated.purge value if defined? Annotated
    Open.lock(info_file, :lock => info_lock) do
      i = info(false)
      i.merge! hash
      dump = Step.serialize_info(i)
      @info_cache = IndiferentHash.setup(i)
      Misc.sensiblewrite(info_file, dump, :force => true, :lock => false) if Open.exists?(info_file)
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
    message = Log.uncolor(message)
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
      str << ": #{ message }" if message and message != :result
      str << " -- #{Log.color :blue, path.to_s}" if path
      str << " #{Log.color :yellow, Process.pid}"
      str
    end
    res = yield
    eend = Time.now
    Log.info do 
      now = Time.now
      str = "#{ Log.color :cyan, status.to_s } +#{Log.color :green, "%.2f" % (eend - start)}"
      str << ": #{ res }" if message == :result
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
    backtrace = ex.backtrace if ex.respond_to?(:backtrace)
    message = ex.message if ex.respond_to?(:message)
    set_info :backtrace, backtrace
    set_info :exception, {:class => ex_class, :message => message, :backtrace => backtrace}
    if msg.nil?
      log :error, "#{ex_class} -- #{message}"
    else
      log :error, "#{msg} -- #{message}"
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
    rec_dependencies = self.rec_dependencies(true)
    return [] if rec_dependencies.empty?
    canfail_paths = self.canfail_paths

    dirty_files = rec_dependencies.reject{|dep|
      (defined?(WorkflowRemoteClient) && WorkflowRemoteClient::RemoteStep === dep) || 
        ! Open.exists?(dep.info_file) ||
        (dep.path && (Open.exists?(dep.path) || Open.remote?(dep.path))) || 
        ((dep.error? || dep.aborted?) && (! dep.recoverable_error? || canfail_paths.include?(dep.path)))
    }
  end

  def dirty?
    return true if Open.exists?(pid_file) && ! ( Open.exists?(info_file) || done? )
    return false unless done? || status == :done
    return false unless ENV["RBBT_UPDATE"] == "true"

    status = self.status

    if done? and not (status == :done or status == :ending or status == :producing) and not status == :noinfo
      return true 
    end

    if status == :done and not done?
      return true 
    end

    if dirty_files.any?
      Log.low "Some dirty files found for #{self.path}: #{Misc.fingerprint dirty_files}"
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
    return false if ! (started? || status == :ending)
    return nil unless Open.exist?(self.pid_file)
    pid = Open.read(self.pid_file).to_i

    return false if done? or error? or aborted? 

    if Misc.pid_exists?(pid) 
      pid
    else
      done? or error? or aborted? 
    end
  end

  def stalled?
    started? && ! (done? || running? || done? || error? || aborted?)
  end

  def missing?
    status == :done && ! Open.exists?(path)
  end

  def error?
    status == :error
  end

  def nopid?
    ! Open.exists?(pid_file) && ! (status.nil? || status == :aborted || status == :done || status == :error || status == :cleaned)
  end

  def aborted?
    status = self.status
    status == :aborted || ((status != :dependencies && status != :cleaned && status != :noinfo && status != :setup && status != :noinfo) && nopid?)
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
    Path.setup(File.join(files_dir, name.to_s), workflow, self)
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
      if Open.exists? dep.info_file
        provenance[dep.path] = dep.provenance if Open.exists? dep.path
      else
        provenance[dep.path] = nil
      end
    end
    {:inputs => info[:inputs], :provenance => provenance}
  end

  def provenance_paths
    provenance = {}
    dependencies.each do |dep|
      provenance[dep.path] = dep.provenance_paths if Open.exists? dep.path
    end
    provenance
  end

  def resumable?
    task && task.resumable
  end

  def config(key, *tokens)
    options = tokens.pop if Hash === tokens.last
    options ||= {}

    new_tokens = []
    if workflow
      workflow_name = workflow.to_s
      new_tokens << ("workflow:" << workflow_name)
      new_tokens << ("task:" << workflow_name << "#" << task_name.to_s)
    end
    new_tokens << ("task:" << task_name.to_s)

    Rbbt::Config.get(key, tokens + new_tokens, options)
  end

  def access
    CMD.cmd("touch -c -h -a #{self.path} #{self.info_file}")
  end
  
  def rec_access
    access
    rec_dependencies.each do |dep|
      dep.access
    end
  end

  def monitor_stream(stream, options = {}, &block)
    case options[:bar] 
    when TrueClass
      bar = progress_bar 
    when Hash
      bar = progress_bar options[:bar]
    when Numeric
      bar = progress_bar :max => options[:bar]
    else
      bar = options[:bar]
    end

    out = if bar.nil?
            Misc.line_monitor_stream stream, &block
          elsif (block.nil? || block.arity == 0)
            Misc.line_monitor_stream stream do
              bar.tick
            end
          elsif block.arity == 1
            Misc.line_monitor_stream stream do |line|
              bar.tick
              block.call line
            end
          elsif block.arity == 2
            Misc.line_monitor_stream stream do |line|
              block.call line, bar
            end
          end

    ConcurrentStream.setup(out, :abort_callback => Proc.new{
      Log::ProgressBar.remove_bar(bar, true) if bar
    }, :callback => Proc.new{
      Log::ProgressBar.remove_bar(bar) if bar
    })

    bgzip = (options[:compress] || options[:gzip]).to_s == 'bgzip'
    bgzip = true if options[:bgzip]

    gzip = true if options[:compress] || options[:gzip]
    if bgzip
      Open.bgzip(out)
    elsif gzip
      Open.gzip(out)
    else
      out
    end
  end

  def relocated?
    done? && info[:path] && info[:path] != path
  end

  def knowledge_base(organism = nil)
    @_kb ||= begin
               kb_dir = self.file('knowledge_base')
               KnowledgeBase.new kb_dir, organism
             end
  end

end
