require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/tsv/excel'

class TestExcel < Test::Unit::TestCase
  def test_xls
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/)
      TmpFile.with_file(nil, true, :extension => 'xlsx') do |excelfile|
        tsv.xls(excelfile)
        new = TSV.xls(excelfile)
        assert_equal %w(row1 row2), new.keys.sort
      end
    end
  end

  def test_xlsx
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/)
      TmpFile.with_file(nil, true, :extension => 'xlsx') do |excelfile|
        tsv.xlsx(excelfile, :sheet => "Sheet1")
        new = TSV.xlsx(excelfile, :sheet => "Sheet1")
        assert_equal %w(row1 row2), new.keys.sort
      end
    end
  end

  def test_excel
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/)
      TmpFile.with_file(nil, true, :extension => 'xlsx') do |excelfile|
        tsv.xlsx(excelfile, :sheet => "Sheet1")
        new = TSV.excel(excelfile, :sheet => "Sheet1")
        assert_equal %w(row1 row2), new.keys.sort
        new = TSV.xlsx(excelfile, :sheet => "Sheet1")
        assert_equal %w(row1 row2), new.keys.sort
      end

      TmpFile.with_file(nil, true, :extension => 'xls') do |excelfile|
        tsv.xls(excelfile, :sheet => "Sheet1")
        new = TSV.excel(excelfile, :sheet => "Sheet1")
        assert_equal %w(row1 row2), new.keys.sort
        new = TSV.xls(excelfile, :sheet => "Sheet1")
        assert_equal %w(row1 row2), new.keys.sort
      end
    end
  end
  
  def test_excel_sheets
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/)

      TmpFile.with_file(nil, false, :extension => 'xlsx') do |excelfile|
        tsv.xlsx(excelfile)
        new = TSV.excel(excelfile)
        assert_equal %w(row1 row2), new.keys.sort

        tsv.xlsx(excelfile, :sheet => "Test")
        new = TSV.excel(excelfile, :sheet => "Test")
        assert_equal %w(row1 row2), new.keys.sort

        tsv.xlsx(excelfile, :sheet => "Test")
        new = TSV.excel(excelfile)
        assert_equal %w(row1 row2), new.keys.sort

        tsv.xlsx(excelfile)
        new = TSV.excel(excelfile, :sheet => "Sheet1")
        assert_equal %w(row1 row2), new.keys.sort
      end


      TmpFile.with_file(nil, false, :extension => 'xls') do |excelfile|
        tsv.xls(excelfile)
        new = TSV.excel(excelfile)
        assert_equal %w(row1 row2), new.keys.sort

        tsv.xls(excelfile, :sheet => "Test")
        new = TSV.excel(excelfile, :sheet => "Test")
        assert_equal %w(row1 row2), new.keys.sort

        tsv.xls(excelfile, :sheet => "Test")
        new = TSV.excel(excelfile)
        assert_equal %w(row1 row2), new.keys.sort

        tsv.xls(excelfile)
        new = TSV.excel(excelfile, :sheet => "Sheet1")
        assert_equal %w(row1 row2), new.keys.sort
      end
    end
  end

  def test_excel_unmerge
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa    b|bb    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/)

      TmpFile.with_file(nil, false, :extension => 'xlsx') do |excelfile|
        tsv.xlsx(excelfile, :unmerge => true)

        new = TSV.excel(excelfile, :merge => true)
        assert_equal %w(a aa), new["row1"]["ValueA"]

        new = TSV.excel(excelfile, :merge => false)
        assert_equal %w(aa), new["row1"]["ValueA"]
      end
    end
  end

  def test_excel_multi_sheets
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv1 = TSV.open(filename, :sep => /\s+/)
      tsv2 = tsv1.annotate(tsv1.dup)
      tsv3 = tsv1.annotate(tsv1.dup)

      tsv2["row2"] = [["AA"], ["BB"], ["Id4"]]
      tsv3["row2"] = [["AAA"], ["BBB"], ["Id5"]]

      TmpFile.with_file(nil, false, :extension => 'xlsx') do |excelfile|
        tsv1.xlsx(excelfile, :sheet => "S1")
        tsv2.xlsx(excelfile, :sheet => "S2", :add_sheet => true)
        workbook = RubyXL::Parser.parse excelfile

        assert_equal %w(S1 S2), workbook.worksheets.collect{|s| s.sheet_name} 

        new = TSV.excel(excelfile, :sheet => "S1")
        assert_equal %w(row1 row2), new.keys.sort
        assert_equal %w(A), new["row2"]["ValueA"]

        new = TSV.excel(excelfile, :sheet => "S2")
        assert_equal %w(row1 row2), new.keys.sort
        assert_equal %w(AA), new["row2"]["ValueA"]

      end
    end
  end
end

