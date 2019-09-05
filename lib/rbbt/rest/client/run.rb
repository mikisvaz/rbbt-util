class WorkflowRESTClient::RemoteStep

  def stream_job(task_url, task_params, stream_input, cache_type = :exec)
    require 'rbbt/util/misc/multipart_payload'
    WorkflowRESTClient.capture_exception do
      @streaming = true

      Log.debug{ "RestClient stream #{Process.pid}: #{ task_url } #{stream_input} #{cache_type} - #{Misc.fingerprint task_params}" }
      res = RbbtMutiplartPayload.issue task_url, task_params, stream_input, nil, nil, true
      type = res.gets

      out = case type.strip
            when "LOCATION"
              @url = res.gets
              @url.sub!(/\?.*/,'')
              join
              WorkflowRESTClient.get_raw(@url)
              @done = true
              @streaming = false
            when /STREAM: (.*)/
              @url = $1.strip
              res.callback = Proc.new do
                Log.medium "Done streaming result from #{@url}"
                @done = true
                @streaming = false
              end
              res
            when "BULK"
              begin
                res.read
              ensure
                @done = true
                @streaming = false
              end
            else
              raise "What? " + type
            end

      ConcurrentStream.setup(out, :filename => @url)

      out
    end
  end

  def execute_job(task_url, task_params, cache_type)
    WorkflowRESTClient.capture_exception do
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
            iii text
            ne = WorkflowRESTClient.parse_exception text
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

        Log.debug{ "RestClient execute: #{ url } - #{Misc.fingerprint task_params}" }
        RestClient::Request.execute(:method => :post, :url => task_url, :payload => task_params, :block_response => bl)
      end

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
    end
  end

  def _run_job(cache_type = :async)
    get_streams

    task_url = URI.encode(File.join(base_url, task.to_s))
    WorkflowRESTClient.__prepare_inputs_for_restclient(inputs)
    task_params = inputs.merge(:_cache_type => cache_type, :jobname => base_name, :_format => [:string, :boolean, :tsv, :annotations].include?(result_type) ? :raw : :json)

    if cache_type == :stream or cache_type == :exec and stream_input and inputs[stream_input]
      io =  self.stream_job(task_url, task_params, stream_input, cache_type) 
      return io
    else
      execute_job(task_url, task_params, cache_type)
    end

  end
end
