require 'net/http'
require 'rbbt-util'

class Net::HTTPGenericRequest
  alias old_send_request_with_body_stream send_request_with_body_stream

  def send_request_with_body_stream(*args)
    if chunked?
      Thread.new do
        old_send_request_with_body_stream(*args)
      end
    else
      old_send_request_with_body_stream(*args)
    end
  end
end

module RbbtMutiplartPayload
  BOUNDARY = "Rbbt_Param_Stream"
  EOL = "\r\n"

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
    io.write "--" + BOUNDARY + EOL + header + EOL  + EOL

    while c = content.read(1024)
      io.write c
    end
    content.close
  end

  def self.close_stream(io)
    io.write "--" + BOUNDARY + "--" + EOL + EOL
    io.write EOL
    io.close
  end

  def self.post_data_stream(inputs = nil, stream_input = nil, stream_io = nil, stream_filename = nil)
    sout, sin = Misc.pipe

    Thread.new do
      inputs.each do |input,content|
        input = input.to_s
        next if stream_input and input == stream_input.to_s
        content_str = case content
                      when String
                        if Misc.is_filename?(content) and File.exists?(content)
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

    sout
  end

  def self.issue(url, inputs = nil, stream_input = nil, stream_io = nil, stream_filename = nil)

    uri = URI(url)
    req = Net::HTTP::Post.new(uri.path)

    IndiferentHash.setup(inputs)

    if stream_input
      stream_io ||= TSV.get_stream inputs[stream_input]
      stream_filename ||= case inputs[stream_input]
                          when String
                            inputs[stream_input]
                          when File
                            inputs[stream_input].filename
                          else
                            'file'
                          end
    end

    sout = RbbtMutiplartPayload.post_data_stream inputs, stream_input, stream_io, stream_filename

    if stream_input
      req.content_type = "multipart/form-data; boundary=" + RbbtMutiplartPayload::BOUNDARY + '; stream=' + stream_input.to_s
      req.body_stream = sout
      req.add_field "Transfer-Encoding", "chunked"
    else
      req.content_type = "multipart/form-data; boundary=" + RbbtMutiplartPayload::BOUNDARY
      req.body = sout.read
    end

    Misc.open_pipe do |sin|
      Net::HTTP.start(uri.hostname, uri.port) do |http|
        http.request(req) do |res|
          if Net::HTTPRedirection === res
            sin.write res["location"]
          elsif stream_input
            res.read_body do |c|
              sin.write c
            end
          else
            sin.write res.body
          end
          sin.close
        end
      end
    end
  end

end