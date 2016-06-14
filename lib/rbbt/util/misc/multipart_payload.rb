require 'net/http'
require 'rbbt-util'

module RbbtMutiplartPayload
  BOUNDARY = "Rbbt_Param_Stream"
  EOL = "\r\n"

  def self.mutex
    @@mutex ||= Mutex.new
  end

  def self.input_header(name, filename = nil)

    if filename
      head_text = 'Content-Disposition: form-data; name="' + name + '"; filename="' + filename + '"'
    else
      head_text = 'Content-Disposition: form-data; name="' + name + '"'
    end

    content_transfer_text = "Content-Transfer-Encoding: binary"

    content_type_text = 'Content-Type: text/plain'

    head_text + EOL + content_transfer_text + EOL + content_type_text + EOL
  end

  def self.add_input(name, content, filename = nil)
    header = input_header(name, filename)
    "--" + BOUNDARY + EOL + header + EOL + content + EOL
  end

  def self.add_stream(io, name, content, filename = nil)
    header = input_header(name, filename)
    io.write "--" + BOUNDARY + EOL + header + EOL

    begin
      while c = content.readpartial(Misc::BLOCK_SIZE)
        io.write c
      end
    rescue EOFError
      io.write "\r\n"
    end
  end

  def self.close_stream(io)
    io.write "--" + BOUNDARY + "--" + EOL + EOL
  end

  def self.post_data_stream(inputs = nil, stream_input = nil, stream_io = nil, stream_filename = nil)
    Misc.open_pipe do |sin|
      inputs.each do |input,content|
        input = input.to_s
        next if stream_input and input == stream_input.to_s
        content_str = case content
                      when String
                        if Misc.is_filename?(content) and File.exist?(content)
                          File.read(content)
                        else
                          content
                        end
                      when File, IO
                        content.read
                      when nil
                        "nil"
                      else
                        content.to_s
                      end
        str = RbbtMutiplartPayload.add_input(input, content_str)
        sin.write str
      end

      RbbtMutiplartPayload.add_stream(sin, stream_input.to_s, stream_io, stream_filename) if stream_input
      RbbtMutiplartPayload.close_stream(sin)
    end
  end

  def self.issue(url, inputs = nil, stream_input = nil, stream_io = nil, stream_filename = nil, report_type = false)

    uri = URI(url)
    IndiferentHash.setup(inputs)

    if stream_input
      stream_io ||= TSV.get_stream inputs[stream_input]
      stream_filename ||= case inputs[stream_input]
                          when String
                            inputs[stream_input]
                          when File
                            inputs[stream_input].path
                          else
                            'file-rand-' + rand(10000000).to_s
                          end
    end

    post_data_stream = RbbtMutiplartPayload.post_data_stream inputs, stream_input, stream_io, stream_filename

    jobname = inputs["jobname"] 

    req = Net::HTTP::Post.new(uri.path)
    if stream_input
      req.content_type = "multipart/form-data; boundary=" + RbbtMutiplartPayload::BOUNDARY + '; stream=' + stream_input.to_s
      req.body_stream = post_data_stream
    else
      req.content_type = "multipart/form-data; boundary=" + RbbtMutiplartPayload::BOUNDARY
      req.body = post_data_stream.read
    end

    req.add_field "Transfer-Encoding", 'chunked'
    req.add_field "RBBT_ID", (jobname || "No name")
    Misc.open_pipe do |sin|
      Net::HTTP.start(uri.hostname, uri.port) do |http|
        http.request(req) do |res|
          if Net::HTTPSuccess === res
            url_path = res["RBBT-STREAMING-JOB-URL"]
            if Net::HTTPRedirection === res
              Log.medium "Response recieved REDIRECT: #{ url_path }"
              sin.puts "LOCATION" if report_type
              sin.write res["location"]
            elsif stream_input and url_path
              Log.medium "Response recieved STREAM: #{ url_path }"
              url = URI::HTTP.build(:host => uri.hostname, :port => uri.port, :path => url_path)
              sin.puts "STREAM: #{url.to_s}" if report_type
              Log.medium "Read body: #{ url_path }"
              res.read_body(sin)
              Log.medium "Read body DONE: #{ url_path }"
            else
              Log.medium "Response recieved BULK: #{ url_path }"
              sin.puts "BULK" if report_type
              sin.write res.body
            end
          else
            raise "Error: #{res.code}"
          end
        end
      end
    end
  end
end
