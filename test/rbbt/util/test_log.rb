require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/util/log'

class TestLog < Test::Unit::TestCase
  def test_get_level
    assert_equal 0, Log.get_level(:debug)
    assert_equal 1, Log.get_level(:low)
    assert_equal 1, Log.get_level("LOW")
    assert_equal 1, Log.get_level(1)
    assert_equal 0, Log.get_level(nil)
  end

  def test_color
    assert Log.color(:green, "green")
  end

  def test_no_stderr
    Log.ignore_stderr do
      STDERR.puts "NOPRINT"
    end
  end

  def test_trap_stderr
    Log.trap_stderr do
      STDERR.puts "NOPRINT"
      STDERR.puts "NOPRINT"
      STDERR.puts "NOPRINT"
      STDOUT.puts "PRINT STDOUT"
      STDOUT.puts "PRINT STDOUT"
    end
    Log.trap_stderr do
      STDERR.puts "NOPRINT"
      STDOUT.puts "PRINT STDOUT"
    end
    Log.trap_std do
      STDERR.puts "NOPRINT"
      STDOUT.puts "NOPRINT STDOUT"
    end
  end

  def test_trap_std
    Log.trap_std do
      STDERR.puts "NOPRINT STDERR"
      STDOUT.puts "NOPRINT STDOUT"
    end
    Log.trap_std "OUT", "ERR", 4, 2 do
      STDERR.puts "NOPRINT STDERR"
      STDOUT.puts "NOPRINT STDOUT"
    end
  end
end

