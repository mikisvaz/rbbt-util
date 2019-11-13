require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/util/python'

class TestPython < Test::Unit::TestCase
  def test_python
    TmpFile.with_file do |tmpdir|
      code =<<-EOF
def python_test(a, b):
	c = a + b
	return c
      EOF
      Open.write(File.join(tmpdir, 'file1.py'), code)
      Open.write(File.join(tmpdir, 'file2.py'), code)
      Open.write(File.join(tmpdir, 'file3.py'), code)
      Open.write(File.join(tmpdir, 'file4.py'), code)
      RbbtPython.add_path tmpdir

      res = nil

      RbbtPython.run 'file2', :python_test do 
        res = python_test(1, 3)
      end
      assert_equal 4, res

      RbbtPython.run do
        pyfrom :file3, :import => :python_test
        res = python_test(1, 4)
      end
      assert_equal 5, res

      RbbtPython.run do
        pyimport :file4
        res = file4.python_test(1, 4)
      end
      assert_equal 5, res

      RbbtPython.run 'file1' do 
        res = file1.python_test(1, 2)
      end
      assert_equal 3, res
    end
  end

  def test_run_log
    Log.severity = 0
    TmpFile.with_file do |tmpdir|
      code =<<-EOF
import sys
def python_print():
	print("Test STDERR", file=sys.stderr)
	print("Test STDOUT")
      EOF
      Open.write(File.join(tmpdir, 'file_print.py'), code)
      RbbtPython.add_path tmpdir

      RbbtPython.run_log 'file_print' do 
        file_print.python_print
      end
      RbbtPython.run_log_stderr 'file_print' do 
        file_print.python_print
      end

      RbbtPython.run_log 'file_print' do 
        file_print.python_print
      end
    end
  end

