require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/util/python'

class TestPython < Test::Unit::TestCase

  def _test_python
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

  def _test_run_log
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

  def _test_keras
    keyword_test :tensorflow do
      defined = RbbtPython.run do
        pyimport "tensorflow.keras.models", as: :km
        defined?(km.Sequential)
      end
      assert defined
    end
  end

  def _test_keras_import
    keyword_test :tensorflow do
      defined = RbbtPython.run do
        pyfrom "tensorflow.keras.models", import: :Sequential
        defined?(self::Sequential)
      end
      assert defined
    end
  end

  def _test_iterate
    a2, b2 = nil, nil
    RbbtPython.run :numpy, as: :np do 
      a = np.array([1,2])
      a2 = RbbtPython.collect a do |e|
        e * 2
      end
      b = PyCall.tuple([1,2])
      b2 = RbbtPython.collect b do |e|
        e * 2
      end
    end
    assert_equal [2,4], a2
    assert_equal [2,4], b2
  end

  def _test_lambda
    l = PyCall.eval "lambda e: e + 2"
    assert_equal 5, l.(3)
  end

  def _test_binding
    raised = false
    RbbtPython.binding_run do
      pyimport :torch
      pyfrom :torch, import: ["nn"]
      begin
        torch
      rescue
        raised = true
      end
    end
    assert ! raised

    raised = false
    RbbtPython.binding_run do
      begin
        torch
      rescue
        raised = true
      end
    end
    assert raised
  end

  def _test_import_method
    random = RbbtPython.import_method :torch, :rand, :random
    assert random.call(1).numpy.to_f > 0
  end

  def _test_class_new_obj
    obj = RbbtPython.class_new_obj("torch.nn", "Module")
    assert_equal "Module()", obj.to_s
  end

  def _test_single
    a = RbbtPython.run_direct :numpy do
      numpy.array([1,2])
    end
    assert a.methods.include? :__pyptr__
  end

  def test_threaded
    a = RbbtPython.run_threaded :numpy do
      numpy.array([1,2])
    end
    assert a.methods.include? :__pyptr__
  end

  def _test_single_lock
    list = %w(TP53 MDM2 BRCA1 KRAS)
    RbbtPython.run :gprofiler do
      res = gprofiler.GProfiler.new(return_dataframe:true).profile(organism:'hsapiens', query: list, sources: %w(GO:BP))

      tsv = RbbtPython.df2tsv res
      tsv.type = :list
      tsv.key_field = "Position"
      tsv = tsv.reorder "native"
      tsv.fields = tsv.fields.collect{|f| f == "p_value" ? "p-value" : f }
      tsv.each do |k,v|
        v[13] = v[13..-1] * "|"
        v.replace v.slice(0, 14)
      end
      res = nil
      tsv
    end
  end

end

