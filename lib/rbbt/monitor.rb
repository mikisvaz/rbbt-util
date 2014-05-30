require 'rbbt'

module Rbbt

  LOCK_DIRS    = Rbbt.share.find_all  + Rbbt.var.cache.persistence.find_all + Rbbt.tmp.tsv_open_locks.find_all + Rbbt.var.jobs.find_all 
  def self.locks(dirs = LOCK_DIRS)
    dirs.collect do |dir|
      dir.glob("**/*.lock")
    end.flatten
  end

  SENSIBLE_WRITE_DIRS = Misc.sensiblewrite_dir.find_all
  def self.sensiblewrites(dirs = SENSIBLE_WRITE_DIRS)
    dirs.collect do |dir|
      dir.glob("**/*").reject{|f| f =~ /\.lock$/ }
    end.flatten
  end

  PERSIST_DIRS    = Rbbt.share.find_all  + Rbbt.var.cache.persistence.find_all  
  def self.persists(dirs = PERSIST_DIRS)
    dirs.collect do |dir|
      dir.glob("**/*.persist").reject{|f| f =~ /\.lock$/ }
    end.flatten
  end

  JOB_DIRS = Rbbt.var.jobs.find_all
  def self.jobs(dirs = JOB_DIRS)
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
