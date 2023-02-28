class Step
  def self.clean(path)
    info_file = Step.info_file path
    pid_file = Step.pid_file path
    md5_file = Step.md5_file path
    files_dir = Step.files_dir path
    tmp_path = Step.tmp_path path

    if ! (Open.writable?(path) && Open.writable?(info_file))
      Log.warn "Could not clean #{path}: not writable"
      return 
    end

    if ENV["RBBT_DEBUG_CLEAN"] == 'true'
      raise "DO NOT CLEAN" 
    end

    if (Open.exists?(path) or Open.broken_link?(path)) or Open.exists?(pid_file) or Open.exists?(info_file) or Open.exists?(files_dir) or Open.broken_link?(files_dir) or Open.exists?(pid_file)

      @result = nil
      @pid = nil

      Misc.insist do
        Open.rm info_file if Open.exists?(info_file)
        Open.rm md5_file if Open.exists?(md5_file)
        Open.rm path if (Open.exists?(path) || Open.broken_link?(path))
        Open.rm_rf files_dir if Open.exists?(files_dir) || Open.broken_link?(files_dir)
        Open.rm pid_file if Open.exists?(pid_file)
        Open.rm tmp_path if Open.exists?(tmp_path)
      end
    end
  end

  def clean
    if ! Open.exists?(info_file)
      Log.high "Refusing to clean step with no .info file: #{path}"
      return self
    end
    status = []
    status << "dirty" if done? && dirty?
    status << "not running" if ! done? && ! running? 
    status.unshift " " if status.any?
    Log.high "Cleaning step: #{path}#{status * " "}"
    Log.stack caller if RBBT_DEBUG_CLEAN
    abort if ! done? && running?
    Step.clean(path)
    @done = false
    self
  end

  def resumable?
    (task && task.resumable) || status == :waiting || status == :cleaned
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
    return true if done? && ! (status == :done || status == :noinfo)
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
    @done ||= path and Open.exists?(path)
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
    status == :aborted || ((status != :ending && status != :dependencies && status != :cleaned && status != :noinfo && status != :setup && status != :noinfo && status != :waiting) && nopid?)
  end

end
