require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
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
    @id = tsv(IDENTIFIERS)
  end

  def test_change_id
    @f1.identifiers = @id
    assert @f1.change_key("X").include? "x"
  end
end
