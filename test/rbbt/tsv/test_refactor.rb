require File.expand_path(__FILE__).sub(%r(/test/.*), '/test/test_helper.rb')
require File.expand_path(__FILE__).sub(%r(.*/test/), '').sub(/test_(.*)\.rb/,'\1')

class TestClass < Test::Unit::TestCase
  def _test_merge_different_rows
    file1 =<<-EOF
row6 dd dd ee
row1 a b c
row2 A B C
row3 1 2 3
   EOF
    file2 =<<-EOF
row20 rr rr
row1 d e
row2 D E
row4 x y z
    EOF
    result =<<-EOF
row1 a b c d e
row2 A B C D E
row20    rr rr
row3 1 2 3  
row4    x y z
row6 dd dd ee  
    EOF

    TmpFile.with_file do |f|
      TSV.merge_different_fields(StringIO.new(file1), StringIO.new(file2), f, :sep => " ", sort: true)
      assert_equal result, Open.read(f).gsub("\t", ' ')
    end
  end

  def test_merge_repeats
    text1=<<-EOF
YHR055C	856452|856450	YHR055C|YHR055C
YPR161C	856290	YPR161C
YOL138C	853982	YOL138C
YDR395W	852004	YDR395W
YGR129W	853030	YGR129W
YPR165W	856294	YPR165W
YPR098C	856213	YPR098C
YPL015C	856092	YPL015C
YCL050C	850307	YCL050C
YAL069W		YAL069W
    EOF

    text2=<<-EOF
YHR055C	CUP1-2	AAA34541
YHR055C	CUP1-2	AAB68382
YHR055C	CUP1-2	AAS56843
YHR055C	CUP1-2	DAA06748
YHR055C	CUP1-2	AAB68384
YHR055C	CUP1-2	AAT93096
YHR055C	CUP1-2	DAA06746
YPR161C	SGV1	BAA14347
YPR161C	SGV1	AAB59314
YPR161C	SGV1	AAB68058
    EOF

    s1 = StringIO.new text1
    s2 = StringIO.new text2
    sss 0 
    TmpFile.with_file do |f|
      TSV.merge_different_fields(s1, s2, f, sort:true, one2one: false)
      ppp Open.read(f).gsub("\t", '·')
    end
  end

end

