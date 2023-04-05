class RemoteStep
  module SSH
    attr_accessor :override_dependencies, :run_type, :slurm_options

    def init_job(cache_type = nil, other_params = {})
      return self if @url
      cache_type = :asynchronous if cache_type.nil? and not @is_exec
      cache_type = :exec if cache_type.nil?
      @last_info_time = nil
      @done = false
      @server, @server_path = RemoteWorkflow::SSH.parse_url base_url
      @input_id ||= "inputs-" << rand(100000).to_s

      if override_dependencies && override_dependencies.any?
        override_dependencies.each do |od|
          name, _sep, value = od.partition("=")
          inputs[name] = value
        end
      end

      inputs.select{|i| Step === i }.each{|i| i.produce }

      RemoteWorkflow::SSH.upload_inputs(@server, inputs, @input_types, @input_id)

      @remote_path ||= Persist.memory("RemoteSteps", :workflow => self, :task => task, :jobname => @name, :inputs => inputs, :cache_type => cache_type) do
        Misc.insist do
          input_types = {}
          RemoteWorkflow::SSH.post_job(File.join(base_url, task.to_s), @input_id, @base_name)
        end
      end
      @name = @remote_path.split("/").last

      if Open.remote?(@name)
        @url = @name
        @name = File.basename(@name)
      else
        @url = File.join(base_url, task.to_s, @name)
      end

      self
    end

    def path
      @server, @server_path = RemoteWorkflow::SSH.parse_url @base_url
      if info[:path]
        "ssh://" + @server + ":" + info[:path]
      elsif @remote_path
        "ssh://" + @server + ":" + @remote_path
      else
        "ssh://" + @server + ":" + ["var/jobs", self.workflow.to_s, task_name.to_s, @name] * "/"
      end
    end
    
    def _run
      RemoteWorkflow::SSH.upload_dependencies(self, @server)
      RemoteWorkflow::SSH.run_job(File.join(base_url, task.to_s), @input_id, @base_name)
    end

    def _run_slurm
      RemoteWorkflow::SSH.run_slurm_job(File.join(base_url, task.to_s), @input_id, @base_name, @slurm_options || {})
    end

    def _orchestrate_slurm
      RemoteWorkflow::SSH.orchestrate_slurm_job(File.join(base_url, task.to_s), @input_id, @base_name, @slurm_options || {})
    end

    def issue
      input_types = {}
      init_job
      @remote_path = case @run_type
                     when 'run', :run, nil
                       _run
                     when 'slurm', :slurm
                       _run_slurm
                     when 'orchestrate', :orchestrate
                       _orchestrate_slurm
                     end
      @started = true
    end

    def produce(*args)
      issue
      while ! (done? || error? || aborted?)
        sleep 1
      end
      raise self.get_exception if error?
      self
    end

    def load
      load_res Open.open(path)
    end

    def run(stream = nil)
      if stream
        issue
      else
        produce
        self.load unless args.first
      end
    end

    def clean
      init_job
      RemoteWorkflow::SSH.clean(@url, @input_id, @base_name)
      _restart
    end

    def abort
      Log.warn "not implemented RemoteWorkflow::SSH.abort(@url, @input_id, @base_name)"
    end

    def input_dependencies
      @input_dependencies ||= inputs.values.flatten.
        select{|i| Step === i || (defined?(RemoteStep) && RemoteStep === i) } + 
        inputs.values.flatten.
        select{|dep| Path === dep && Step === dep.resource }.
        select{|dep| ! dep.resource.started? }. # Ignore input_deps already started
        collect{|dep| dep.resource }
    end
  end
end

