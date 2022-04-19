require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/util/misc/communication'

class TestCommunication < Test::Unit::TestCase
  def test_send_email
    to = from = 'mvazque2@localhost'
    subject = message = "Test"
    iii Misc.send_email(to, from, subject, message)
  end
end

