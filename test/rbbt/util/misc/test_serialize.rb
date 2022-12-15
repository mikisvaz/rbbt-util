require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/util/misc/serialize'

class TestClass < Test::Unit::TestCase
  def test_load_yaml
    yaml_txt=<<-EOF
---
a: b
    EOF
    yaml_sio = StringIO.new yaml_txt

    assert_equal "b", Misc.load_yaml(yaml_txt)["a"]
    assert_equal "b", Misc.load_yaml(yaml_sio)["a"]

    TmpFile.with_file yaml_txt do |yaml_file|
      assert_equal "b", Misc.load_yaml(yaml_file)["a"]
      Open.open(yaml_file) do |yaml_io|
        assert_equal "b", Misc.load_yaml(yaml_io)["a"]
      end
    end

  end
end

