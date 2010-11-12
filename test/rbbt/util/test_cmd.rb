require File.dirname(__FILE__) + '/../../test_helper'
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
end
