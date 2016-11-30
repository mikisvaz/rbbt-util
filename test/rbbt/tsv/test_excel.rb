require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/tsv/excel'

class TestExcel < Test::Unit::TestCase
  def _test_xls
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

  def _test_xlsx
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

  def _test_excel
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
  
  def _test_excel_sheets
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

        new = TSV.excel(excelfile, :merge => false)
        assert_equal %w(a), new["row1"]["ValueA"]

        new = TSV.excel(excelfile, :merge => true)
        assert_equal %w(a aa), new["row1"]["ValueA"]
      end
    end
  end
end

