require 'rest-client'

module WorkflowRESTClient
  def self.__prepare_inputs_for_restclient(inputs)
    inputs.each do |k,v|
      if v.respond_to? :path and not v.respond_to? :original_filename
        class << v
          def original_filename
            File.expand_path(path)
          end
        end
      end

      if Array === v and v.empty?
        inputs[k] = "EMPTY_ARRAY"
      end
    end
  end

  def workflow_description
    WorkflowRESTClient.get_raw(File.join(url, 'description'))
  end

  def documentation
    @documention ||= IndiferentHash.setup(WorkflowRESTClient.get_json(File.join(url, "documentation"),{}))
  end

  def self.task_info(url, task)
    @@task_info ||= {}

    key = [url, task] * "#"
    @@task_info[key] ||= begin
                           task_info = WorkflowRESTClient.get_json(File.join(url, task.to_s, 'info'))
                           task_info = WorkflowRESTClient.fix_hash(task_info)

                           task_info[:result_type] = task_info[:result_type].to_sym
                           task_info[:export] = task_info[:export].to_sym
                           task_info[:input_types] = WorkflowRESTClient.fix_hash(task_info[:input_types], true)
                           task_info[:inputs] = task_info[:inputs].collect{|input| input.to_sym }

                           @@task_info[key] = task_info
                         end
  end

  def task_info(task)
    WorkflowRESTClient.task_info(url, task)
  end

  def exported_tasks
    (@asynchronous_exports  + @synchronous_exports + @exec_exports).compact.flatten
  end

  def tasks
    @tasks ||= Hash.new do |hash,task_name| 
      info = task_info(task_name)
      task = Task.setup info do |*args|
        raise "This is a remote task" 
      end
      task.name = task_name.to_sym
      hash[task_name] = task
    end
  end

  def load_tasks
    exported_tasks.each{|name| tasks[name]}
    nil
  end

  def task_dependencies
    @task_dependencies ||= Hash.new do |hash,task| 
      hash[task] = if exported_tasks.include? task
        WorkflowRESTClient.get_json(File.join(url, task.to_s, 'dependencies'))
      else
        []
      end
    end
  end

  def init_remote_tasks
    task_exports = WorkflowRESTClient.get_json(url)
    @asynchronous_exports = task_exports["asynchronous"].collect{|task| task.to_sym }
    @synchronous_exports = task_exports["synchronous"].collect{|task| task.to_sym }
    @exec_exports = task_exports["exec"].collect{|task| task.to_sym }
    @stream_exports = task_exports["stream"].collect{|task| task.to_sym }
    @can_stream = task_exports["can_stream"]
  end

  def self.execute_job(base_url, task, task_params, cache_type)
    self.capture_exception do
      task_url = URI.encode(File.join(base_url, task.to_s))

      sout, sin = Misc.pipe

      post_thread = Thread.new(Thread.current) do |parent|
        bl = lambda do |rok|
          if Net::HTTPOK === rok
            _url = rok["RBBT-STREAMING-JOB-URL"]
            @url = File.join(task_url, File.basename(_url)) if _url
            rok.read_body do |c,_a, _b|
              sin.write c
            end
            sin.close
          else
            err = StringIO.new
            rok.read_body do |c,_a, _b|
              err.write c
            end
            text = begin
                     reader = Zlib::GzipReader.new(err)
                     reader.read
                   rescue
                     err.rewind
                     err.read
                   end
            ne = @adaptor.parse_exception text
            case ne
            when String
              parent.raise e.class, ne
            when Exception
              parent.raise ne
            else
              parent.raise "Error in RestClient: " << rok.message
            end
          end
        end

        task_params.each do |k,v|
          task_params[k] = v.read if IO === v
        end

        Log.debug{ "RestClient execute: #{ task_url } - #{Misc.fingerprint task_params}" }
        RestClient::Request.execute(:method => :post, :url => task_url, :payload => task_params, :block_response => bl)
      end

      # It seems like now response body are now decoded by Net::HTTP after 2.1
      # https://github.com/rest-client/rest-client/blob/cf3e5a115bcdb8f3344aeac0e45b44d67fac1a42/history.md
      decode = Gem.loaded_specs["rest-client"].version < Gem::Version.create('2.1')
      if decode
        reader = Zlib::GzipReader.new(sout)
        res_io = Misc.open_pipe do |sin|
          while c = reader.read(Misc::BLOCK_SIZE)
            sin.write c
          end
          sin.close
          @done = true
        end
        ConcurrentStream.setup(res_io, :threads => [post_thread]) do
          @done = true
          @streaming = false
        end
      else
        ConcurrentStream.setup(sout, :threads => [post_thread]) do
          @done = true
          @streaming = false
        end
      end

    end
  end

end
