class Step
  def info(check_lock = true)
    return {:status => :noinfo} if info_file.nil? || ! Open.exists?(info_file)

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

  def load_inputs_from_info
    if info[:inputs]
      info_inputs = info[:inputs]
      if task && task.respond_to?(:inputs) && task.inputs
        IndiferentHash.setup info_inputs
        @inputs = NamedArray.setup info_inputs.values_at(*task.inputs.collect{|name| name.to_s}), task.inputs
      else
        if Hash === info_inputs
          @inputs = NamedArray.setup info_inputs.values, info_inputs.keys
        else
          @inputs = info_inputs
        end
      end
    else
      nil
    end
  end

  def load_dependencies_from_info
    relocated = nil
    @dependencies = (self.info[:dependencies] || []).collect do |task,name,dep_path|
      dep_path = task if dep_path.nil?
      if Open.exists?(dep_path) || Open.exists?(dep_path + '.info')
        Workflow._load_step dep_path
      else
        next if FalseClass === relocated
        new_path = Workflow.relocate(path, dep_path)
        relocated = true if Open.exists?(new_path) || Open.exists?(new_path + '.info')
        Workflow._load_step new_path
      end
    end.compact
    @relocated = relocated
  end


  def archive_deps
    self.set_info :archived_info, archived_info
    self.set_info :archived_dependencies, info[:dependencies]
  end

  def archived_info
    return info[:archived_info] if info[:archived_info]

    archived_info = {}
    dependencies.each do |dep|
      if Symbol === dep.overriden && ! Open.exists?(dep.info_file)
        archived_info[dep.path] = dep.overriden
      else
        archived_info[dep.path] = dep.info
      end
      archived_info.merge!(dep.archived_info)
    end if dependencies

    archived_info
  end

  def archived_inputs
    return {} unless info[:archived_dependencies]
    archived_info = self.archived_info

    all_inputs = IndiferentHash.setup({})
    deps = info[:archived_dependencies].collect{|p| p.last}
    seen = []
    while path = deps.pop
      dep_info = archived_info[path]
      if Hash === dep_info
        dep_info[:inputs].each do |k,v|
          all_inputs[k] = v unless all_inputs.include?(k)
        end if dep_info[:inputs]
        deps.concat(dep_info[:dependencies].collect{|p| p.last } - seen) if dep_info[:dependencies]
        deps.concat(dep_info[:archived_dependencies].collect{|p| p.last } - seen) if dep_info[:archived_dependencies]
      end
      seen << path
    end

    all_inputs
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
    end || []
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
    options = Misc.add_defaults options, :severity => Log::INFO, :file => (@exec ? nil : path)
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

  def progress_bar(msg = "Progress", options = nil, &block)
    if Hash === msg and options.nil?
      options = msg
      msg = nil
    end
    options = {} if options.nil?

    max = options[:max]
    bar = Log::ProgressBar.new_bar(max, {:desc => msg, :file => (@exec ? nil : file(:progress))}.merge(options))

    if block_given?
      bar.init
      res = yield bar
      bar.remove
      res
    else
      bar
    end
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
      ! (klass <= RbbtException )
    rescue Exception
      true
    end
  end

end
