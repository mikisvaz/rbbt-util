require 'rbbt/util/misc'

class Task
  class Job
    attr_accessor :task, :id, :name, :options, :previous_jobs, :required_files, :pid, :path, :previous_jobs, :input

    IDSEP = "_"

    def self.id2name(job_id)
      job_id.split(IDSEP)
    end

    def self.load(task, id)
      name, hash = id2name(id)
      job = self.new task, id, name, nil, nil
      job.load_dependencies
      job
    end

    def initialize(task, id, name, options = nil, previous_jobs = nil, required_files = nil, input = nil)
      @task = task  
      @id =id
      @name = name
      @options = options || {}
      @previous_jobs = previous_jobs || []
      @required_files = required_files || []
      @input = input

      basedir = task.workflow.jobdir unless task.workflow.nil?
      @path = File.join(basedir || Task.basedir, task.name, id)
    end

    def previous_jobs_rec
      return [] if previous_jobs.nil?
      previous_jobs + previous_jobs.collect{|job| job.previous_jobs_rec}.flatten
    end

    def previous_jobs=(previous_jobs)
      @previous_jobs = previous_jobs
      @all_inputs = nil
    end

    def all_inputs
      if true or not defined? @all_inputs
        @all_inputs = {}
        previous_jobs_rec.each do |job| @all_inputs[job.task.name] = job end
        @all_inputs.extend IndiferentHash
        @all_inputs
      else
        @all_inputs
      end
    end

    def input(name = nil)
      if name.nil?
        if @input.nil?
          nil
        else
          @input.load
        end
      else
        all_inputs[name]
      end
    end 

    def previous_jobs
      if @previous_jobs.nil?
        nil
      else
        NamedArray.name @previous_jobs, @previous_jobs.collect{|job| job.task.name} 
      end
    end

    def info_file
      path + '.info'
    end

    def info
      return {} if not File.exists?(info_file)
      info = YAML.load(File.open(info_file)) || {}
      info.extend IndiferentHash
    end

    def set_info(key, value)
      Misc.lock(info_file, key, value) do |info_file, key, value| i = self.info
        new_info = i.merge(key => value)
        Open.write(info_file, new_info.to_yaml)
      end
    end

    def step(name = nil, message = nil)
      @previous_jobs
      if name.nil?
        info[:step]
      else
        set_info(:step, name)
        if message.nil?
          Log.info "[#{task.name}] Step '#{name}'"
        else
          Log.info "[#{task.name}] Step '#{name}': #{message.chomp}"
          set_info(:messages, info[:messages] || [] << message) if not message.nil?
        end
      end
    end

    def messages
      info[:messages] || []
    end

    def files(file = nil, data = nil)
      return Dir.glob(File.join(path + '.files/*')).collect{|f| File.basename(f)} if file.nil?

      filename = Resource::Path.path(File.join(path + '.files', file.to_s))
      if data.nil?
        filename
      else
        Open.write(filename, data)
      end
    end

    def abort
      if @pid
        Process.kill("INT", @pid)
      end
    end

    def done?
      [:done, :error, :aborted].include? info[:step]
    end

    def error?
      step == :error or step == :aborted
    end

    def aborted?
      step == :aborted 
    end

    def arguments
      options.values_at *task.options
    end

    def block
      task.block
    end

    def run_dependencies
      required_files.each do |file| file.produce unless File.exists? file end unless required_files.nil?
      previous_jobs.each do |job| 
        if not job.recursive_done? 
          job.clean if job.error?
          job.start
          job.step :done unless job.step == :error or job.step == :aborted
        end
      end unless previous_jobs.nil?
    end

    def save_dependencies
      set_info :previous_jobs, @previous_jobs.collect{|job| "JOB:#{job.task.name}/#{job.id}"}  unless @previous_jobs.nil?
      set_info :required_files, @required_files.collect{|file| file.responds_to? :find ? file.find : file} if @required_files.nil?
    end

    def load_dependencies
      @previous_jobs = info[:previous_jobs].collect do |job_string| 
        job_string =~ /JOB:(.*)\/(.*)/
        task.workflow.load_job($1, $2)
      end if info[:previous_jobs]
      @required_files = info[:required_files] if info[:required_files]
    end

    def start
      begin
        run_dependencies

        Log.medium("[#{task.name}] Starting Job '#{ name }'. Path: '#{ path }'")
        set_info(:start_time, Time.now)
        save_options(options)
        save_dependencies

        extend task.scope unless task.scope.nil? or Object == task.scope.class

        result = instance_exec *arguments, &block

        if not result.nil?
          case task.persistence
          when nil, :string, :tsv, :integer
            Open.write(path, result.to_s)
          when :marshal
            Open.write(path, Marshal.dump(result))
          when :yaml
            Open.write(path, YAML.dump(result))
          end
        end

        set_info(:end_time, Time.now)
        Log.medium("[#{task.name}] Finished Job '#{ name }'. Path: '#{ path }'")
      rescue Exception
        set_info(:exception_backtrace, $!.backtrace)
        step(:error, "#{$!.class}: #{$!.message}")
        raise $!
      end
    end

    def save_options(options)
      new_options = {}
      options.each do |key, value|
        case 
        when TSV === value
          new_options[key] = value.to_s
        else
          new_options[key] = value
        end
      end
      set_info(:options, new_options)
    end

    def recursive_done?
      (previous_jobs || []).inject(true){|acc,j| acc and j.recursive_done?} and done? and not error? 
    end

    def run
      return self if recursive_done?
      begin
        step(:started)
        start
        step(:done)
      rescue Exception
        Log.debug $!.message
        Log.debug $!.backtrace * "\n"
        step(:error, "#{$!.class}: #{$!.message}")
      end
      self
    end

    def fork
      return self if recursive_done?
      @pid = Process.fork do
        begin
          step(:started)
          start
          step(:done)
        rescue Exception
          Log.debug $!.message
          Log.debug $!.backtrace * "\n"
          step(:error, "#{$!.class}: #{$!.message}")
        end
        exit
      end

      self
    end

    def join
      if @pid.nil?
        while not done? do
          Log.debug "Waiting: #{info[:step]}"
          sleep 5
        end
      else
        Process.waitpid @pid
      end

      self
    end

    def open
      File.open(path)
    end

    def read
      File.open(path) do |f| f.read end
    end

    def load(*args)
      case task.persistence
      when :float
        Open.read(path).to_f
      when :integer
        Open.read(path).to_i
      when :string
        Open.read(path)
      when :tsv
        TSV.new(path, *args)
      when :marshal
        Marshal.load(Open.read(path))
      when :yaml
        YAML.load(Open.read(path))
      when nil
        nil
      end
    end

    def clean
      FileUtils.rm path if File.exists? path
      FileUtils.rm info_file if File.exists? info_file
      FileUtils.rm_rf path + '.files' if File.exists? path + '.files'
      self
    end

    def recursive_clean
      previous_jobs.each do |job| job.recursive_clean end unless previous_jobs.nil?
      clean
    end
  end # END Job
end


