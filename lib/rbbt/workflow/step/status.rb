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

    if (Open.exists?(path) or Open.broken_link?(path)) or Open.exists?(pid_file) or Open.exists?(info_file) or Open.exists?(files_dir) or Open.broken_link?(files_dir)

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
    self
  end

  def resumable?
    (task && task.resumable) || status == :waiting || status == :cleaned
  end

end
