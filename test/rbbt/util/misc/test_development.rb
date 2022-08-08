require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/util/misc/development'

class TestMiscDevelopment < Test::Unit::TestCase
  def test_ssh
    Misc.ssh_run 'mn1' do
      puts "hola"
    end
  end

  def __test_timeout
    Misc.timeout_insist(2) do
      puts "Start"
      3.times do
        puts "ping"
        sleep rand(4)
        puts "pong"
      end
    end
  end
end

