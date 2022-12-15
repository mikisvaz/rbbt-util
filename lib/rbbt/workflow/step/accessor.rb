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
            j.soft_grace
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
    path.nil? ? nil : Path.setup(path + '.files')
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


  def name
    @name ||= path.sub(/.*\/#{Regexp.quote task_name.to_s}\/(.*)/, '\1')
  end


  def short_path
    [task_name, name] * "/"
  end

  def short_path_real
    [(Symbol === overriden ? overriden : task_name).to_s, name] * "/"
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

  def init_info(force = false)
    return nil if @exec || info_file.nil? || (Open.exists?(info_file) && ! force)
    batch_job = info[:batch_job] if Open.exists?(info_file)
    batch_system = info[:batch_system] if Open.exists?(info_file)
    Open.lock(info_file, :lock => info_lock) do
      i = {:status => :waiting, :pid => Process.pid, :path => path, :real_inputs => real_inputs, :overriden => overriden}
      i[:batch_job] = batch_job if batch_job
      i[:batch_system] = batch_system if batch_system
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
      value = Annotated.purge(value)

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

  def file(name=nil)
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
      Misc.load_yaml(file(name))
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
