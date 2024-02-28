require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/util/misc/communication'

class TestCommunication < Test::Unit::TestCase
  def test_send_email
    keyword_test :mail do
      to = from = 'mvazque2@localhost'
      subject = message = "Test"
      Misc.send_email(to, from, subject, message)
    end
  end
end

