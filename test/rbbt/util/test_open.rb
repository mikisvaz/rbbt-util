require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'rbbt/util/open'
require 'rbbt/util/tmpfile'
require 'test/unit'
require 'iconv'

class TestOpen < Test::Unit::TestCase

  def test_wget
    assert(Misc.fixutf8(Open.wget('http://google.com', :quiet => true).read) =~ /html/)
  end

  def test_nice
    nice =  0.5

    Open.wget('http://google.com', :quiet => true, :nice => nice).read =~ /html/
    t = Time.now
    Open.wget('http://google.com', :quiet => true, :nice => nice).read =~ /html/
    assert(Time.now - t + 0.5 >= nice)

    Open.wget('http://google.com', :quiet => true, :nice => nice, :nice_key => 1).read =~ /html/
    Open.wget('http://google.com', :quiet => true, :nice => nice, :nice_key => 2).read =~ /html/
    t = Time.now
    Open.wget('http://google.com', :quiet => true, :nice => nice, :nice_key => 1).read =~ /html/
    assert(Time.now - t + 0.5 >= nice)
  end

  def test_remote?
    assert(Open.remote?('http://google.com'))
    assert(! Open.remote?('~/.bashrc'))
  end

  def test_open
    assert(Open.read('http://google.com', :quiet => true) =~ /html/)
  end

  def test_read
    content =<<-EOF
1
2
3
4
    EOF
    TmpFile.with_file(content) do |file|
      sum = 0
      Open.read file do |line| sum += line.to_i end
      assert_equal(1 + 2 + 3 + 4, sum)
      assert_equal(content, Open.read(file))
    end
  end

   def test_read_grep
    content =<<-EOF
1
2
3
4
    EOF
    TmpFile.with_file(content) do |file|
      sum = 0
      Open.read(file, :grep => '^1\|3') do |line| sum += line.to_i end
      assert_equal(1 + 3, sum)
    end

    TmpFile.with_file(content) do |file|
      sum = 0
      Open.read(file, :grep => ["1","3"]) do |line| sum += line.to_i end
      assert_equal(1 + 3, sum)
    end
 
  end

  def test_gzip
    content =<<-EOF
1
2
3
4
    EOF
    TmpFile.with_file(content) do |file|
      `gzip #{file}`
      assert_equal(content, Open.read(file + '.gz'))
      FileUtils.rm file + '.gz'
    end
  end

  
end

