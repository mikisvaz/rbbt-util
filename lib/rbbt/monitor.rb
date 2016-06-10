require 'rbbt'

module Rbbt

  #LOCK_DIRS = Rbbt.share.find_all  + Rbbt.var.cache.persistence.find_all +  Rbbt.var.jobs.find_all +
  #  Rbbt.tmp.tsv_open_locks.find_all + Rbbt.tmp.persist_locks.find_all + Rbbt.tmp.sensiblewrite_lock_dir.find_all +
  #  Rbbt.tmp.produce_locks.find_all + Rbbt.tmp.step_info_locks.find_all
  LOCK_DIRS = Rbbt.tmp.tsv_open_locks.find_all + Rbbt.tmp.persist_locks.find_all + Rbbt.tmp.sensiblewrite_lock_dir.find_all +
    Rbbt.tmp.produce_locks.find_all + Rbbt.tmp.step_info_locks.find_all

  SENSIBLE_WRITE_DIRS = Misc.sensiblewrite_dir.find_all

  PERSIST_DIRS    = Rbbt.share.find_all  + Rbbt.var.cache.persistence.find_all

  JOB_DIRS = Rbbt.var.jobs.find_all

  MUTEX_FOR_THREAD_EXCLUSIVE = Mutex.new

  def self.dump_memory(file, obj = nil)
    Log.info "Dumping #{obj} objects into #{ file }"
    Thread.new do
      while true
        Open.write(file) do |f|
          MUTEX_FOR_THREAD_EXCLUSIVE.synchronize do
            GC.start
            ObjectSpace.each_object(obj) do |o|
              f.puts "---"
              f.puts(String === o ? o : o.inspect)
            end
          end
        end
        FileUtils.cp file, file + '.save'
        sleep 3
      end
    end
  end

  def self.file_time(file)
    info = {}
    begin
      info[:ctime] = File.ctime file
      info[:atime] = File.atime file
      info[:elapsed] = Time.now - info[:ctime]
    rescue Exception
    end
    info[:ctime] = Time.now - 999
    info
  end

  #{{{ LOCKS

  def self.locks(dirs = LOCK_DIRS)
    dirs.collect do |dir|
      next unless Open.exists? dir
      `find "#{ dir }" -name "*.lock" 2>/dev/null`.split "\n"
    end.compact.flatten
  end

  def self.lock_info(dirs = LOCK_DIRS)
    lock_info = {}
    locks(dirs).each do |f|
      lock_info[f] = {}
      begin
        lock_info[f].merge!(file_time(f))
        if File.size(f) > 0
          info = Open.open(f) do |s|
            YAML.load(s)
          end
          IndiferentHash.setup(info)
          lock_info[f][:pid] = info[:pid]
          lock_info[f][:ppid] = info[:ppid]
        end
      rescue Exception
        #Log.exception $!
      end
    end
    lock_info
  end

  #{{{ SENSIBLE WRITES

  def self.sensiblewrites(dirs = SENSIBLE_WRITE_DIRS)
    dirs.collect do |dir|
      next unless Open.exists? dir
      `find "#{ dir }" -not -name "*.lock" -not -type d 2>/dev/null`.split "\n"
    end.compact.flatten
  end

  def self.sensiblewrite_info(dirs = SENSIBLE_WRITE_DIRS)
    info = {}
    sensiblewrites(dirs).each do |f|
      begin
        i = file_time(f)
        info[f] = i
      rescue
        Log.exception $!
      end
    end
    info
  end

  # PERSISTS

  def self.persists(dirs = PERSIST_DIRS)
    dirs.collect do |dir|
      next unless Open.exists? dir
      `find "#{ dir }" -name "*.persist" 2>/dev/null`.split "\n"
    end.compact.flatten
  end

  def self.persist_info(dirs = PERSIST_DIRS)
    info = {}
    persists(dirs).each do |f|
      begin
        i = file_time(f)
        info[f] = i
      rescue
        Log.exception $!
      end
    end
    info
  end

  # PERSISTS

  def self.job_info(workflows = nil, tasks = nil, dirs = JOB_DIRS)
    require 'rbbt/workflow/step'

    workflows = [workflows] if workflows and not Array === workflows
    workflows = workflows.collect{|w| w.to_s} if workflows

    tasks = [tasks] if tasks and not Array === tasks
    tasks = tasks.collect{|w| w.to_s} if tasks

    jobs = {}
    dirs.collect do |dir|
      next unless Open.exists? dir

      dir.glob("*").collect do |workflowdir|
        workflow = File.basename(workflowdir)
        next if workflows and not workflows.include? workflow

        workflowdir.glob("*").collect do |taskdir|
          task = File.basename(taskdir)
          next if tasks and not tasks.include? task

          files = `find "#{ taskdir }/" -not -type d -not -path "*/*.files/*" 2>/dev/null`.split("\n").sort
          _files = Set.new files
          TSV.traverse files, :type => :array, :into => jobs do |file|
            if m = file.match(/(.*).info$/)
              file = m[1]
            end

            name = file[taskdir.length+1..-1]
            info_file = file + '.info'

            info = {}

            info[:workflow] = workflow
            info[:task] = task
            info[:name] = name

            if _files.include? file
              info = info.merge(file_time(file))
              info[:done] = true
              info[:info_file] = File.exists?(info_file) ? info_file : nil
            else
              info = info.merge({:info_file => info_file, :done => false})
            end

            [file, info]
          end

        end.compact.flatten
      end.compact.flatten
    end.compact.flatten
    jobs
  end

  # REST

  def self.__jobs(dirs = JOB_DIRS)
    job_files = {}
    dirs.each do |dir|
      workflow_dirs = dir.glob("*").each do |wdir|
        workflow = File.basename(wdir)
        job_files[workflow] = {}
        task_dirs = wdir.glob('*')
        task_dirs.each do |tdir|
          task = File.basename(tdir)
          job_files[workflow][task] = tdir.glob('*')
        end
      end
    end
    jobs = {}
    job_files.each do |workflow,task_jobs|
      jobs[workflow] ||= {}
      task_jobs.each do |task, files|
        jobs[workflow][task] ||= {}
        files.each do |f|
          next if f =~ /\.lock$/
            job = f.sub(/\.(info|files)/,'')

          jobs[workflow][task][job] ||= {}
          if jobs[workflow][task][job][:status].nil?
            status = nil
            status = :done if Open.exists? job
            if status.nil? and f=~/\.info/
              info = begin
                       Step::INFO_SERIALIAZER.load(Open.read(f, :mode => 'rb'))
                     rescue
                       {}
                     end
            status = info[:status]
            pid = info[:pid]
            end

            jobs[workflow][task][job][:pid] = pid if pid
            jobs[workflow][task][job][:status] = status if status
          end
        end
      end
    end
    jobs
  end

  def self.load_lock(lock)
    begin
      info = Misc.insist 3 do
        YAML.load(Open.read(lock))
      end
      info.values_at "pid", "ppid", "time"
    rescue Exception
      time = begin
               File.atime(lock)
             rescue Exception
               Time.now
             end
      [nil, nil, time]
    end
  end

end
