class Step
  class ResourceManager
    class NotEoughResources < Exception
    end

    attr_accessor :cpus, :memory
    def initialize(cpus = nil, memory = nil)
      @cpus = cpus
      @memory = memory
      @sem_file = "ResourceManager-" + rand(10000).to_s
      @semaphore = RbbtSemaphore.create_semaphore(@sem_file, 1)
    end

    def allocate(cpus = nil, memory = nil, &block)
      RbbtSemaphore.synchronize(@semaphore) do
        if (@cpus && cpus && @cpus < cups)  ||
          (@memory && memory && @memory < memory) 
          raise NotEoughResources
        end
        begin
          @cpus -= cpus
          @memory -= memory
          yield
        rescue
          @cpus += cpus
          @memory += memory
        end
      end
    end

    def finalize(manager)
      RbbtSemaphore.delete_semaphore(@sem_file)
    end

    def self.finalize(manager)
      proc { manager.finalize }
    end
  end

  class Scheduler
    attr_accessor :jobs, :cpus, :dep_jobs, :job_deps, :jobps
    def initialize(jobs, cpus)
      @jobs = jobs
      @cpus = cpus
      
      @job_deps = {}

      with_deps = jobs.dup
      @dep_jobs = {}
      @job_deps = {}
      @jobps = {}
      @missing = Set.new
      while with_deps.any?
        job = with_deps.pop
        @jobps[job.path] = job
        @missing << job.path unless job.done?

        jdeps = job.dependencies
        jdeps += job.inputs.flatten.select{|i| Step === i}

        jdeps.reject!{|dep| dep.done? }
        @job_deps[job.path] = []
        jdeps.each do |dep|
          next if dep.done?
          @dep_jobs[dep.path] ||= []
          @job_deps[job.path] << dep.path
          @dep_jobs[dep.path] << job.path
          with_deps << dep unless @job_deps.include? dep.path
        end
      end


      def self.ready
        @job_deps.select do |jobp,deps|
          (@missing & deps).empty?
        end.collect{|jobp,deps| jobp}
      end

      def self.next
        priorities = {}
        @jobs.each do |job|
          priorities = 1
        end

        @missing.each do |jobp|
        end

        @dep_jobsb
      end
    end
  end

  def self._priorities(jobs)
    job_level = {}
    jobs.each do |job|
      job_level[job.path] = 1.0
    end

    with_deps = jobs.dup
    dep_jobs = {}
    job_deps = {}
    while with_deps.any?
      job = with_deps.pop
      level = job_level[job.path]
      job_deps[job.path] = []
      jdeps = job.dependencies
      jdeps += job.inputs.flatten.select{|i| Step === i}

      jdeps.reject!{|dep| dep.done? }
      jdeps.each do |dep|
        next if dep.done?
        dep_jobs[dep.path] ||= []
        job_level[dep.path] = level / (10 * jdeps.length) if job_level[dep.path].nil? || job_level[dep.path] < level / (10 * jdeps.length)
        job_deps[job.path] << dep.path
        dep_jobs[dep.path]  << job.path
        with_deps << dep unless job_deps.include? dep.path
      end
    end
    [job_level, job_deps, dep_jobs]
  end

  def self.produce_jobs(jobs, cpus, step_cpus = {})
    require 'fc'

    step_cpus = IndiferentHash.setup(step_cpus || {})

    deps = []

    jobs = [jobs] unless Array === jobs

    job_level, job_deps, dep_jobs = self._priorities(jobs)

    jobps = {}
    (jobs + jobs.collect{|job| job.rec_dependencies}).flatten.uniq.each do |job|
      jobps[job.path] = job
    end

    prio_queue = FastContainers::PriorityQueue.new :max

    job_deps.each do |jobp,depps|
      next if depps.any?
      level = job_level[jobp]

      prio_queue.push(jobp, level) 
    end

    queue = RbbtProcessQueue.new cpus

    missing = job_deps.keys
    queue.callback do |jobp|
      Log.info "Done: #{jobp}"
      missing -= [jobp]

      job_level, job_deps, dep_jobs = self._priorities(jobs)

      parentsp = dep_jobs[jobp]

      parentsp.each do |parentp|
        next unless job_deps[parentp].include? jobp
        job_deps[parentp] -= [jobp]
        if job_deps[parentp].empty?
          level = job_level[parentp]
          prio_queue.push(parentp, level )
        end
      end if parentsp
      prio_queue_new = FastContainers::PriorityQueue.new :max
      while prio_queue.any?
        elem = prio_queue.pop
        prio_queue_new.push(elem, job_level[elem])
      end
      prio_queue = prio_queue_new
    end
    
    queue.init do |jobp|
      Log.info "Processing: #{jobp}"
      job = jobps[jobp]
      job_cpus = step_cpus[job.task_name] || 1
      sleep 0.5
      #job.produce
      jobp
    end

    while missing.any?
      while prio_queue.empty? && missing.any?
        sleep 1
      end
      break if missing.empty?
      jobp = prio_queue.pop
      queue.process jobp
    end

    queue.join
  end
end


if __FILE__ == $0
  require 'rbbt/workflow'

  module TestWF
    extend Workflow
    input :num, :integer
    task :dep => :integer do |num|
      num
    end
    dep :dep, :num => 1
    dep :dep, :num => 2
    dep :dep, :num => 3
    task :test do 
      dependencies.collect{|d| d.load.to_s}  * ","
    end
  end
  Log.severity = 0
  job = TestWF.job(:test)
  job.recursive_clean

  Rbbt::Config.load_file Rbbt.etc.config_profile.HTS.find
  Workflow.require_workflow "Sample"
  Workflow.require_workflow "HTS"

  jobs = []
  jobs << Sample.job(:mutect2, "QUINTANA-15")
  jobs << Sample.job(:mutect2, "QUINTANA-25")
  jobs << Sample.job(:mutect2, "QUINTANA-28")

  sched = Step::Scheduler.new(jobs, 3)
end
