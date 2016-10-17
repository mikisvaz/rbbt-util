require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'rbbt/util/cmd'
require 'test/unit'

class TestCmd < Test::Unit::TestCase

  def test_cmd_option_string
    assert_equal("--user-agent firefox", CMD.process_cmd_options("--user-agent" => "firefox"))
    assert_equal("--user-agent=firefox", CMD.process_cmd_options("--user-agent=" => "firefox"))
    assert_equal("-q", CMD.process_cmd_options("-q" => true))
    assert_equal("", CMD.process_cmd_options("-q" => nil))
    assert_equal("", CMD.process_cmd_options("-q" => false))

    assert(CMD.process_cmd_options("--user-agent" => "firefox", "-q" => true) =~ /--user-agent firefox/)
    assert(CMD.process_cmd_options("--user-agent" => "firefox", "-q" => true) =~ /-q/)
  end

  def test_cmd
    assert_equal("test\n", CMD.cmd("echo '{opt}' test").read)
    assert_equal("test", CMD.cmd("echo '{opt}' test", "-n" => true).read)
    assert_equal("test2\n", CMD.cmd("cut", "-f" => 2, "-d" => '" "', :in => "test1 test2").read)
  end

  def test_pipe
    assert_equal("test\n", CMD.cmd("echo test", :pipe => true).read)
    assert_equal("test\n", CMD.cmd("echo '{opt}' test", :pipe => true).read)
    assert_equal("test", CMD.cmd("echo '{opt}' test", "-n" => true, :pipe => true).read)
    assert_equal("test2\n", CMD.cmd("cut", "-f" => 2, "-d" => '" "', :in => "test1 test2", :pipe => true).read)
  end

  def test_error
    assert_raise ProcessFailed do CMD.cmd('fake-command') end
    assert_raise ProcessFailed do CMD.cmd('ls -fake_option') end

    assert_raise ProcessFailed do CMD.cmd('fake-command', :stderr => true) end
    assert_raise ProcessFailed do CMD.cmd('ls -fake_option', :stderr => true) end
 
    assert_nothing_raised ProcessFailed do CMD.cmd('fake-command', :stderr => false, :pipe => true) end
    assert_nothing_raised ProcessFailed do CMD.cmd('ls -fake_option', :stderr => false, :pipe => true) end
 
    assert_raise ProcessFailed do CMD.cmd('fake-command', :stderr => true, :pipe => true).join end
    assert_raise ProcessFailed do CMD.cmd('ls -fake_option', :stderr => true, :pipe => true).join end
  end

  def test_pipes
    text = <<-EOF
line1
line2
line3
line11
line22
line33
    EOF

    TmpFile.with_file(text * 100) do |file|

      Open.open(file) do |f|
        io = CMD.cmd('tail -n 10', :in => f, :pipe => true)
        io2 = CMD.cmd('head -n 10', :in => io, :pipe => true)
        io3 = CMD.cmd('head -n 10', :in => io2, :pipe => true)
        assert_equal 10, io3.read.split(/\n/).length
      end
    end
  end

end
