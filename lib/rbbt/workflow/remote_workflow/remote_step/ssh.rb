class RemoteStep
  module SSH

    DEFAULT_REFRESH_TIME = 2

    attr_accessor :override_dependencies, :run_type, :batch_options, :produce_dependencies

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

      @remote_path ||= Persist.memory("RemoteStep", :workflow => self.workflow, :task => task, :jobname => @name, :inputs => inputs, :cache_type => cache_type) do
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
      RemoteWorkflow::SSH.upload_dependencies(self, @server, 'user', @produce_dependencies)
      RemoteWorkflow::SSH.run_job(File.join(base_url, task.to_s), @input_id, @base_name)
    end

    def _run_batch
      RemoteWorkflow::SSH.upload_dependencies(self, @server, 'user', @produce_dependencies)
      RemoteWorkflow::SSH.run_batch_job(File.join(base_url, task.to_s), @input_id, @base_name, @batch_options || {})
    end

    def _orchestrate_batch
      RemoteWorkflow::SSH.orchestrate_batch_job(File.join(base_url, task.to_s), @input_id, @base_name, @batch_options || {})
    end

    def issue
      input_types = {}
      init_job
      @remote_path = case @run_type
                     when 'run', :run, nil
                       _run
                     when 'batch', :batch
                       _run_batch
                     when 'orchestrate', :orchestrate
                       _orchestrate_batch
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
        self.load unless stream
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
        #select{|dep| ! dep.resource.started? }. # Ignore input_deps already started
        collect{|dep| dep.resource }
    end
  end
end

