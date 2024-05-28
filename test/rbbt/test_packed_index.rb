require File.join(File.expand_path(File.dirname(__FILE__)), '..', 'test_helper.rb')
require 'rbbt/packed_index'

class TestPackedIndex < Test::Unit::TestCase
  def test_packed_index

    TmpFile.with_file do |tmpfile|
      pi = PackedIndex.new tmpfile, true, %w(i i 23s f f f f f)
      100.times do |i|
        pi << [i, i+2, i.to_s * 10, rand, rand, rand, rand, rand]
      end
      pi << nil
      pi << nil
      pi.close
      pi = PackedIndex.new(tmpfile, false)
      100.times do |i|
        assert_equal i, pi[i][0] 
        assert_equal i+2, pi[i][1] 
      end
      assert_equal nil, pi[100]
      assert_equal nil, pi[101]
    end
  end

  def _test_bgzip

    size = 1000000
    density = 0.1

    access = []
    (size * density).to_i.times do
      access << rand(size-1) + 1
    end
    access.sort!
    access.uniq!

    TmpFile.with_file do |tmpfile|
      pi = PackedIndex.new tmpfile, true, %w(i i 23s f f f f f)
      size.times do |i|
        pi << [i, i+2, i.to_s * 10, rand, rand, rand, rand, rand]
      end
      pi << nil
      pi << nil
      pi.close

      pi = PackedIndex.new(tmpfile, false)
      Misc.benchmark do
        access.each do |point|
          assert_equal point+2, pi[point][1]
        end
      end

      `bgzip #{tmpfile} `
      `mv #{tmpfile}.gz #{tmpfile}.bgz`

      pi = PackedIndex.new(tmpfile + '.bgz', false)
      pi[0]
      Misc.benchmark do
        access.each do |point|
          assert_equal point+2, pi[point][1]
        end
      end
    end
  end

  def __test_benchmark

    TmpFile.with_file do |tmpfile|
      pi = PackedIndex.new tmpfile, true, %w(i i 23s f f f f f)
      100.times do |i|
        pi << [i, i+2, i.to_s * 10, rand, rand, rand, rand, rand]
      end
      pi << nil
      pi << nil
      pi.close
      pi = PackedIndex.new(tmpfile, false)
      Misc.benchmark(1000) do
        100.times do |i|
          assert_equal i, pi[i][0] 
          assert_equal i+2, pi[i][1] 
        end
      end
    end
  end

end

