require File.join(File.expand_path(File.dirname(__FILE__)), '../../', 'test_helper.rb')
require 'rbbt/tsv'
require 'rbbt/tsv/field_index'

class TestTSVFieldIndex < Test::Unit::TestCase

  def test_field_index
    content =<<-EOF
#Id    ValueA    ValueB ValueC
rowA    A|AA    B|BB  C|CC
rowa    a|aa    b|BB  C|CC
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(File.open(filename), :sep => /\s+/, :type => :double)
      assert_equal ["rowA"], tsv.field_index("ValueA")["A"]
    end
  end
end
