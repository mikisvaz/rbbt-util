require File.expand_path(__FILE__).sub(%r(/test/.*), '/test/test_helper.rb')
require File.expand_path(__FILE__).sub(%r(.*/test/), '').sub(/test_(.*)\.rb/,'\1')

class TestSSW < Test::Unit::TestCase
  def test_ssw
    s1 = "ACTGACTG"
    s2 = "ACGGACTG"
    assert SmithWaterman.alignment_percent(s1, s2) > 0.5
    assert SmithWaterman.alignment_percent(s1, s2) < 1
  end
end

