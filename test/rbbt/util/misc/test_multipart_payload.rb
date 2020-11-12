require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/util/misc/multipart_payload'

class TestMultipartPayload < Test::Unit::TestCase

  URL='http://localhost:2887/Echo'
  def _test_post_data_stream
    content =<<-EOF
Line 1
Line 2
Line 3
END
    EOF
    mutipart =<<-EOF
--Rbbt_Param_Stream<>
Content-Disposition: form-data; name="input1"<>
Content-Transfer-Encoding: binary<>
Content-Type: text/plain<>
<>
Input1<>
--Rbbt_Param_Stream<>
Content-Disposition: form-data; name="input2"<>
Content-Transfer-Encoding: binary<>
Content-Type: text/plain<>
<>
Input2<>
--Rbbt_Param_Stream<>
Content-Disposition: form-data; name="stream_input"<>
Content-Transfer-Encoding: binary<>
Content-Type: text/plain<>
<>
Line 1
Line 2
Line 3
END
<>
--Rbbt_Param_Stream--<>
<>
    EOF
    TmpFile.with_file(content) do |tmpfile|
      inputs = {}
      stream_input = :stream_input
      inputs[:input1] = "Input1"
      inputs[:input2] = "Input2"
      inputs[:stream_input] = Open.open(tmpfile)

      post_data_stream = RbbtMutiplartPayload.post_data_stream(inputs, stream_input, inputs[stream_input])
      assert_equal mutipart, post_data_stream.read.gsub(/\r\n/,"<>\n")
    end
  end

  def _test_issue
    content =<<-EOF
Line 1
Line 2
Line 3
END
    EOF
    mutipart =<<-EOF
--Rbbt_Param_Stream<>
Content-Disposition: form-data; name="input1"<>
Content-Transfer-Encoding: binary<>
Content-Type: text/plain<>
<>
Input1<>
--Rbbt_Param_Stream<>
Content-Disposition: form-data; name="input2"<>
Content-Transfer-Encoding: binary<>
Content-Type: text/plain<>
<>
Input2<>
--Rbbt_Param_Stream<>
Content-Disposition: form-data; name="stream_input"; filename="FILENAME"<>
Content-Transfer-Encoding: binary<>
Content-Type: text/plain<>
<>
Line 1
Line 2
Line 3
END
<>
--Rbbt_Param_Stream--<>
<>
DONE_PARAM_STREAM
    EOF
    inputs = {}
    #mutipart.gsub!('<>',"\r")
    stream_input = :stream_input
    inputs[:input1] = "Input1"
    inputs[:input2] = "Input2"
    num = 50
    cpus = 1
    TmpFile.with_file(content) do |tmpfile|
      inputs  = inputs.dup
      inputs[:stream_input] = File.open(tmpfile)

     io = RbbtMutiplartPayload.issue(URL, inputs, stream_input)
      assert_equal mutipart.sub("FILENAME", tmpfile).force_encoding("ASCII"), io.read.gsub(/\r\n/,"<>\n") 
    end
  end

  def _test_issue_multiple
    content =<<-EOF
Line 1
Line 2
Line 3
END
    EOF
    mutipart =<<-EOF
--Rbbt_Param_Stream<>
Content-Disposition: form-data; name="input1"<>
Content-Transfer-Encoding: binary<>
Content-Type: text/plain<>
<>
Input1<>
--Rbbt_Param_Stream<>
Content-Disposition: form-data; name="input2"<>
Content-Transfer-Encoding: binary<>
Content-Type: text/plain<>
<>
Input2<>
--Rbbt_Param_Stream<>
Content-Disposition: form-data; name="stream_input"; filename="FILENAME"<>
Content-Transfer-Encoding: binary<>
Content-Type: text/plain<>
<>
Line 1
Line 2
Line 3
END
<>
--Rbbt_Param_Stream--<>
<>
DONE_PARAM_STREAM
    EOF
    inputs = {}
    stream_input = :stream_input
    inputs[:input1] = "Input1"
    inputs[:input2] = "Input2"
    num = 100
    cpus = 10
    Misc.bootstrap((0..num-1).to_a, cpus) do  |n|
      TmpFile.with_file(content) do |tmpfile|
        inputs  = inputs.dup
        inputs[:stream_input] = File.open(tmpfile)

        io = RbbtMutiplartPayload.issue(URL, inputs, stream_input)
        str = io.read
        assert_equal mutipart.sub("FILENAME", tmpfile).force_encoding("ASCII"), str.gsub(/\r\n/,"<>\n") 
      end
    end
  end

  def _test_raw
    mutipart =<<-EOF
--Rbbt_Param_Stream<>
Content-Disposition: form-data; name="input1"<>
Content-Transfer-Encoding: binary<>
Content-Type: text/plain<>
<>
Input1<>
--Rbbt_Param_Stream<>
Content-Disposition: form-data; name="input2"<>
Content-Transfer-Encoding: binary<>
Content-Type: text/plain<>
<>
Input2<>
--Rbbt_Param_Stream<>
Content-Disposition: form-data; name="stream_input"; filename="FILENAME"<>
Content-Transfer-Encoding: binary<>
Content-Type: text/plain<>
<>
Line 1
Line 2
Line 3
END
<>
--Rbbt_Param_Stream--<>
<>
    EOF
    mutipart.gsub!('<>',"\r")
    inputs = {}
    stream_input = :stream_input
    inputs[:input1] = "Input1"
    inputs[:input2] = "Input2"
    num = 50
    cpus = 1

    Misc.bootstrap((0..num-1).to_a, cpus) do  |n|
      puts mutipart
      TmpFile.with_file(mutipart, false) do |tmpfile|
        inputs  = inputs.dup
        inputs[:stream_input] = File.open(tmpfile)

        puts "wget '#{URL}' --post-file #{ tmpfile } -O -"
        io = CMD.cmd("wget '#{URL}' --post-file #{ tmpfile } -O -", :pipe => true)
        assert_equal mutipart, io.read
      end
    end
  end
end

