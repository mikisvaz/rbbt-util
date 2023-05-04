require File.expand_path(__FILE__).sub(%r(/test/.*), '/test/test_helper.rb')
require File.expand_path(__FILE__).sub(%r(.*/test/), '').sub(/test_(.*)\.rb/,'\1')

class TestProcPath < Test::Unit::TestCase
  def test_record_and_plot
    Log.with_severity 0 do
      pid = Process.fork do
        a = ""
        (0..100).each do |i|
          a << (0..rand(10000).to_i).to_a.collect{|i| "TEST #{i}" } * " "
          sleep 0.01
        end
      end

      TmpFile.with_file(nil, false) do |db|

        ProcPath.record(pid, db, :interval => '1', "recnum" => 100)
        ProcPath.plot(db, db + '.svg', "moving-average-window" => 1 )
        assert Open.exist?(db + '.svg')
      end
    end
  end
end

