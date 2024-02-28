require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/util/procpath'

class TestProcPath < Test::Unit::TestCase
  def test_record_and_plot
    keyword_test :procpath do
      pid = Process.fork do
        a = ""
        (0..1000).each do 
          a << (0..rand(10000).to_i).to_a.collect{|i| "TEST #{i}" } * " "
          sleep 0.1
        end
      end

      TmpFile.with_file(nil, false) do |db|

        ProcPath.record(pid, db, :interval => '1', "recnum" => 100)
        ProcPath.plot(db, db + '.svg', "moving-average-window" => 1 )
      end
    end
  end
end

