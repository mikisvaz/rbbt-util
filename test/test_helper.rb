require 'test/unit'
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
$LOAD_PATH.unshift(File.dirname(__FILE__))

class Test::Unit::TestCase
  def test_datafile(file)
    File.join(File.dirname(__FILE__), 'data', file)
  end
end
