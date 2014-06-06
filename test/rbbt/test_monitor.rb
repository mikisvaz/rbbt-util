require File.join(File.expand_path(File.dirname(__FILE__)), '..', 'test_helper.rb')
require 'rbbt/monitor'

class TestMonitor < Test::Unit::TestCase
  def _test_locks
  end
end

if __FILE__ == $0
  iii Rbbt.lock_info.select{|k,v| k =~ /.file_repo/}

end

