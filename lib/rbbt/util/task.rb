class Task
  class << self
    attr_accessor :tasks, :basedir
  end

  Task.basedir = "."

  class Job
    attr_reader :task, :id, :name, :options

    attr_reader :pid, :path

    def initialize(task, id, name, options)
      @task = task  
      @id =id
      @name = name
      @options = options

      @path = File.join(task.basedir, id)
    end

    def info_file
      path + '.info'
    end

    def info
      return {} if not File.exists? info_file
      YAML.load(File.open(info_file))
    end

    def set_info(key, value)
      Open.write info_file, YAML.dump(info.merge(key => value))
    end

    def step(name)
      set_info(:step, name)
    end

    def arguments
      options.values_at *task.options
    end

    def block
      task.block
    end

    def start
       instance_exec *arguments, &block
    end

    def fork
      @pid = Process.fork do
        step(:started)
        set_info(:options, options)
        result = start
        Open.write(path, result.to_s) unless result.nil?
        step(:done)
        exit
      end
      self
    end

    def join
      if @pid.nil?
        while info[:step] != :done do
          sleep 5
        end
      else
        Process.waitpid @pid
      end

      self
    end

    def read
      File.open(path) do |f| f.read end
    end
  end

  def job_options(run_options = nil)
    return {} if options.nil?

    job_options = {}
    options.each do |option|
      job_options[option] = Misc.process_options run_options, option
    end

    job_options
  end

  def job_id(name, job_options)
    if job_options.any?
      name.to_s + "_" + Misc.hash2md5(job_options)
    else
      name.to_s
    end
  end

  attr_reader :name, :basedir, :options, :option_defaults, :option_descriptions,  :block
  def initialize(name, basedir = nil, options = nil, option_defaults = nil, option_descriptions = nil, &block)
    @name = name.to_s
    @basedir = basedir || File.join(Task.basedir, name.to_s)

    @options = Array === options ? options : [options] unless options.nil? 

    @option_defaults = option_defaults unless option_defaults.nil? 
    @option_descriptions = option_descriptions unless option_descriptions.nil? 
    @block = block unless not block_given?
  end
  
  def job(jobname, run_options = {})

    job_id = self.job_id jobname, run_options

    job_options = self.job_options run_options

    Job.new(self, job_id, jobname, job_options)
  end

  def run(*args)
    job(*args).start
  end
end
