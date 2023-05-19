require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt-util'
require 'rbbt/tsv'
require 'rbbt/tsv/change_id'

class TestTSVChangeID < Test::Unit::TestCase

  FILE1=<<-EOF
#: :sep=' '
#A B C
a b c
aa bb cc
  EOF

  FILE2=<<-EOF
#: :sep=' '
#X Y Z
x y z
xx yy zz
  EOF

  IDENTIFIERS=<<-EOF
#: :sep=' '
#A X
a x
aa xx
  EOF

  def tsv(text, options = {})
    options = Misc.add_defaults options, :type => :list
    TSV.open StringIO.new(text), options
  end

  def setup
    @f1 = tsv(FILE1)
    @f2 = tsv(FILE2)
    @f3 = tsv(FILE2, :key_field => "Y")
    @id = tsv(IDENTIFIERS)
  end

  def test_swap_id
    @f3.identifiers = @id
    assert_equal "a", @f3.swap_id("X","A")["y"]["A"]
  end


  def _test_change_key
    @f1.identifiers = @id
    assert @f1.change_key("X").include? "x"
  end

  def _test_translate_key
    @f1.identifiers = @id
    assert TSV.translate(@f1, @f1.key_field, "X", :persist => false).include? "x"
  end

  def _test_translate_key_persist
    @f1.identifiers = @id
    assert TSV.translate(@f1, @f1.key_field, "X", :persist => true).include? "x"
  end
end
