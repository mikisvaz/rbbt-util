require File.expand_path(__FILE__).sub(%r(/test/.*), '/test/test_helper.rb')
require File.expand_path(__FILE__).sub(%r(.*/test/), '').sub(/test_(.*)\.rb/,'\1')

class TestPythonScript < Test::Unit::TestCase
  def test_script
    result = RbbtPython.script <<-EOF, variables: {value: 2}
result = value * 3
    EOF
    assert_equal 6, result
  end
end

