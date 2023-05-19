require File.expand_path(__FILE__).sub(%r(/test/.*), '/test/test_helper.rb')
require File.expand_path(__FILE__).sub(%r(.*/test/), '').sub(/test_(.*)\.rb/,'\1')

class TestMiscRefactor < Test::Unit::TestCase
  def test_refactor
    TmpFile.with_file do |file|
      Misc.lock file do
        assert true
      end
    end
  end
end

