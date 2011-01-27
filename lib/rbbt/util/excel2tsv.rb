require 'spreadsheet'
require 'rbbt/util/tsv'
require 'rbbt/util/tmpfile'
class TSV
  def self.excel2tsv(file, options = {})
    sheet = options.delete :sheet
    header = options.delete :header
    header = true unless header == false
    sheet ||= 0
    TmpFile.with_file do |filename|
      workbook = Spreadsheet.open Open.open(file)
      sheet    = workbook.worksheet sheet

      rows = []

      sheet.each do |row|
        rows << row.values_at(0..(row.size - 1))
      end

      File.open(filename, 'w') do |f|
        if header
          header = rows.shift
          f.puts "#" + header * "\t"
        end

        rows.each do |row| f.puts row * "\t" end
      end

      TSV.new(filename, options)
    end
  end
end
