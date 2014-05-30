require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')

require 'rbbt-util'
require 'rbbt/util/misc/lock'

class TestLock < Test::Unit::TestCase
  def __test_stress
    size = 1000000
    num = 50
    cpus = 200
    TmpFile.with_file do |dir|
      TSV.traverse (0..size).to_a, :cpus => cpus, :type => :array, :bar => true do |i|
        begin
          v = rand(num)
          file = File.join(dir, "file-" << v.to_s)
          Persist.persist("foo", :string, :file => file, :update => true) do
            Process.pid.to_s
          end

          txt = `ls -la /proc/#{Process.pid}/fd |grep deleted`
          Open.write(file, txt)
          puts [Process.pid, txt.split("\n").length] * ": "
        rescue Exception
          Log.exception $!
          raise $!
        end
      end
    end
  end
end

