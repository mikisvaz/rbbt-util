class RemoteStep
  module SSH
    attr_accessor :override_dependencies

    def init_job(cache_type = nil, other_params = {})
      cache_type = :asynchronous if cache_type.nil? and not @is_exec
      cache_type = :exec if cache_type.nil?
      @last_info_time = nil
      @done = false
      @server, @server_path = RemoteWorkflow::SSH.parse_url base_url
      @input_id ||= "inputs-" << rand(100000).to_s

      if override_dependencies

        if override_dependencies && override_dependencies.any?
          override_dependencies.each do |od|
            name, _sep, value = od.partition("=")
            inputs[name] = value
          end
        end

        RemoteWorkflow::SSH.upload_inputs(@server, inputs, @input_types, @input_id)
      else
        RemoteWorkflow::SSH.upload_inputs(@server, inputs, @input_types, @input_id)
      end

      @name ||= Persist.memory("RemoteSteps", :workflow => self, :task => task, :jobname => @name, :inputs => inputs, :cache_type => cache_type) do
        Misc.insist do
          input_types = {}
          RemoteWorkflow::SSH.post_job(File.join(base_url, task.to_s), @input_id, @base_name)
        end
      end
      if Open.remote? @name
        @url = @name
        @name = File.basename(@name)
      else
        @url = File.join(base_url, task.to_s, @name)
      end
      self
    end

    def path
      @server, @server_path = RemoteWorkflow::SSH.parse_url @base_url
      "ssh://" + @server + ":" + @remote_path
    end

    def produce(*args)
      input_types = {}
      init_job
      @remote_path = RemoteWorkflow::SSH.run_job(File.join(base_url, task.to_s), @input_id, @base_name)
      while ! done?
        sleep 1
      end
    end

    def load
      load_res Open.open(path)
    end

    def run(*args)
      produce(*args)
      self.load unless args.first
    end

    def clean
      init_job
      RemoteStep::SSH.clean(@url, @input_id, @base_name) if done?
      _restart
    end

  end
end

