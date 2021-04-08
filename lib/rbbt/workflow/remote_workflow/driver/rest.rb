require 'rest-client'
class RemoteWorkflow
  module REST

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

    def self.encode(url)
      begin
        URI::DEFAULT_PARSER.escape(url)
      rescue
        Log.warn $!.message
        url
      end
    end

    def self.clean_url(url, params = {})
      params = params.merge({ :_format => 'json', :update => 'clean' })
      params = RemoteWorkflow.fix_params params
      res = RemoteWorkflow.capture_exception do
        Misc.insist(2, 0.5) do
          Log.debug{ "RestClient clean: #{ url } - #{Misc.fingerprint params}" }
          res = begin 
                  RestClient.get(self.encode(url), :params => params)
                rescue RestClient::NotFound
                  return nil
                end
          raise TryAgain if res.code == 202
          res
        end
      end
      res
    end

    def self.get_raw(url, params = {})
      params = params.merge({ :_format => 'raw' })
      params = RemoteWorkflow.fix_params params
      res = RemoteWorkflow.capture_exception do
        Misc.insist(2, 0.5) do
          raise "No url" if url.nil?
          Log.debug{ "RestClient get_raw: #{ url } - #{Misc.fingerprint params}" }
          res = RestClient.get(self.encode(url), :params => params)
          raise TryAgain if res.code == 202
          res.to_s
        end
      end
      res
    end

    def self.get_json(url, params = {})
      Log.debug{ "RestClient get_json: #{ url } - #{Misc.fingerprint params }" }
      params = params.merge({ :_format => 'json' })
      params = RemoteWorkflow.fix_params params

      res = RemoteWorkflow.capture_exception do
        Misc.insist(2, 0.5) do
          RestClient.get(self.encode(url), :params => params)
        end
      end

      begin
        JSON.parse(res)
      rescue
        res
      end
    end

    def self.post_jobname(url, params = {})
      Log.debug{ "RestClient post_jobname: #{ url } - #{Misc.fingerprint params}" }
      params = params.merge({ :_format => 'jobname' })
      params = RemoteWorkflow.fix_params params

      RemoteWorkflow::REST.__prepare_inputs_for_restclient(params)
      name = RemoteWorkflow.capture_exception do
        begin
          RestClient.post(self.encode(url), params)
        rescue RestClient::MovedPermanently, RestClient::Found, RestClient::TemporaryRedirect
          raise RbbtException, "REST end-point moved to: #{$!.response.headers[:location]}"
        end
      end

      Log.debug{ "RestClient jobname returned for #{ url } - #{Misc.fingerprint params}: #{name}" }

      name
    end

    def self.post_json(url, params = {})
      if url =~ /_cache_type=:exec/
        JSON.parse(Open.open(url, :nocache => true))
      else
        params = params.merge({ :_format => 'json' })
        params = fix_params params

        res = RemoteWorkflow.capture_exception do
          RestClient.post(self.encode(url), params)
        end

        begin
          JSON.parse(res)
        rescue
          res
        end
      end
    end

    def self.task_info(url, task)
      @@task_info ||= {}

      key = [url, task] * "#"
      @@task_info[key] ||= begin
                             task_info = RemoteWorkflow::REST.get_json(File.join(url, task.to_s, 'info'))
                             task_info = RemoteWorkflow.fix_hash(task_info)

                             task_info[:result_type] = task_info[:result_type].to_sym
                             task_info[:export] = task_info[:export].to_sym
                             task_info[:input_types] = RemoteWorkflow.fix_hash(task_info[:input_types], true)
                             task_info[:inputs] = task_info[:inputs].collect{|input| input.to_sym }

                             task_info
                           end
    end

    def self.execute_job(base_url, task, task_params, cache_type)
      RemoteWorkflow.capture_exception do
        task_url = URI.encode(File.join(base_url, task.to_s))

        sout, sin = Misc.pipe

        post_thread = Thread.new(Thread.current) do |parent|
          bl = lambda do |rok|
            case rok
            when Net::HTTPOK
              _url = rok["RBBT-STREAMING-JOB-URL"]
              @url = File.join(task_url, File.basename(_url)) if _url
              rok.read_body do |c,_a, _b|
                sin.write c
              end
              sin.close
            when Net::HTTPRedirection, Net::HTTPAccepted
              Thread.current.report_on_exception = false
              raise TryThis.new(rok)
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
              ne = RemoteWorkflow.parse_exception text
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
          begin
            RestClient::Request.execute(:method => :post, :url => task_url, :payload => task_params, :block_response => bl)
          rescue TryThis
            url = $!.payload["location"]
            RestClient::Request.execute(:method => :get, :url => url, :block_response => bl)
          end
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


    def task_info(task)
      RemoteWorkflow::REST.task_info(url, task)
    end

    def init_remote_tasks
      task_exports = IndiferentHash.setup(RemoteWorkflow::REST.get_json(url))
      @asynchronous_exports = (task_exports["asynchronous"] || []).collect{|task| task.to_sym }
      @synchronous_exports = (task_exports["synchronous"] || []).collect{|task| task.to_sym }
      @exec_exports = (task_exports["exec"] || []).collect{|task| task.to_sym }
      @stream_exports = (task_exports["stream"] || []).collect{|task| task.to_sym }
      @can_stream = task_exports["can_stream"]
    end
  end
end
