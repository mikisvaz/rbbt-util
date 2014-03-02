require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/util/simpleopt/setup'

class TestSOPTSetup < Test::Unit::TestCase
  def setup
    SOPT.inputs = nil
    SOPT.input_types = nil
    SOPT.input_descriptions = nil
    SOPT.input_shortcuts = nil
  end

  def test_setup
    SOPT.setup <<-EOF
Test application

$ test cmd -arg 1

It does some imaginary stuff

-a--arg* Argument
-a2--arg2* Argument

    EOF

    assert_equal "test cmd -arg 1", SOPT.synopsys
    assert SOPT.inputs.include? "arg"
    assert SOPT.inputs.include? "arg2"
  end

  def test_setup_alt
    SOPT.setup <<-EOF
Test application

It does some imaginary stuff

-a--arg* Argument
-a2--arg2* Argument

    EOF

    assert SOPT.inputs.include? "arg"
    assert SOPT.inputs.include? "arg2"
  end

  def test_setup_alt2
    SOPT.setup <<-EOF
Test application

-a--arg* Argument
-a2--arg2* Argument

    EOF

    assert SOPT.inputs.include? "arg"
    assert SOPT.inputs.include? "arg2"
  end

  def test_setup_alt3
    SOPT.setup <<-EOF
Pulls the values from a tsv colum

$ rbbt tsv values [options] <filename.tsv|->

Use - to read from STDIN

-tch--tokyocabinet File is a tokyocabinet hash database
-tcb--tokyocabinet_bd File is a tokyocabinet B database
-h--help Print this help
    EOF

    assert SOPT.inputs.include? "tokyocabinet"
  end

end


