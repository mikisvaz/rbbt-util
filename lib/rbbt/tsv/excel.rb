require 'spreadsheet'
require 'rubyXL'

module TSV

  def self._remove_link(value)
    if value =~ /<([\w]+)[^>]*>(.*?)<\/\1>/
      $2
    else
      value
    end
  end

  def self._clean_float(v)
    case v
    when Float
      v.to_s.sub(/e(-?\d+)$/,'E\1')
    when String
      if v =~ /^-?[\d\.]+e(-?\d+)$/
        v.sub(/e(-?\d+)$/,'E\1') 
      else
        v
      end
    else
      v
    end
  end


  def self._excel_data(tsv, options ={})
    options = Misc.add_defaults options, :sep2 => ', '

    name = Misc.process_options options, :name
    sep2 = Misc.process_options options, :sep2
    unmerge = Misc.process_options options, :unmerge
    sort_by = Misc.process_options options, :sort_by
    sort_by_cast = Misc.process_options options, :sort_by_cast
    remove_links = Misc.process_options options, :remove_links

    i = 1
    if sort_by
      if sort_by_cast
        data = tsv.sort_by sort_by do |k, v| 
          if Array === v
            v.first.send(sort_by_cast)
          else
            v.send(sort_by_cast)
          end
        end
      else
        data = tsv.sort_by sort_by
      end
    else
      data = tsv
    end

    rows = []
    data.through do |key, values|
      cells = []
      cells.push((name and key.respond_to?(:name)) ?  key.name || key : key )

      values = [values] unless Array === values
      values.each do |value|
        v = (name and value.respond_to?(:name)) ?  value.name || value : value 
        if Array === v
          v = v.collect{|_v| _remove_link(_v)} if remove_links
          v = v.collect{|_v| _clean_float(_v)} 
          if unmerge
            cells.push v
          else
            cells.push v * sep2
          end
        else
          v = _remove_link(v) if remove_links
          cells.push v
        end
      end

      rows << cells
      i += 1
    end
    if unmerge
      new_rows = []
      rows.each do |row|
        header = row.shift
        Misc.zip_fields(row).each do |values|
          new_rows << [header] + values
        end
      end
      rows = new_rows
    end
    [tsv.all_fields, rows]
  end

  module XLS
    def self.read(file, options = {})
      options = Misc.add_defaults options, :sep2 => /[,|]\s?/
      sheet = Misc.process_options options, :sheet
      header = Misc.process_options options, :header

      header = true unless header == false
      sheet ||= 0
      TmpFile.with_file do |filename|
        workbook = Spreadsheet.open Open.open(file)
        sheet    = workbook.worksheet sheet

        rows = []

        sheet.each do |row|
          rows << row.values_at(0..(row.size - 1)).collect{|c| String === c ? c.gsub("\n", ' ') : c }
        end

        num_values = rows.first.length
        File.open(filename, 'w') do |f|
          if header
            header = rows.shift
            f.puts "#" + header * "\t"
          end

          rows.each do |row| 
            values =  row.collect{|c| c.respond_to?(:value) ? c.value : c }
            values[num_values-1] ||= nil
            f.puts values * "\t"
          end
        end

        TSV.open(filename, options)
      end
    end

    def self.write(tsv, file, options = {})
      options = Misc.add_defaults options, :sheet => "Sheet1"
      sheet = Misc.process_options options, :sheet
      fields, rows = TSV._excel_data(tsv, options)

      book = Spreadsheet::Workbook.new
      sheet1 = book.create_worksheet 
      sheet1.name = sheet if sheet

      sheet1.row(0).concat fields

      rows.each_with_index do |cells,i|
        sheet1.row(i+1).concat cells
      end

      book.write file
    end
  end

  module XLSX
    def self.read(file, options = {})
      options = Misc.add_defaults options, :sep2 => /[,|]\s?/
      sheet = Misc.process_options options, :sheet
      header = Misc.process_options options, :header

      header = true unless header == false
      TmpFile.with_file do |filename|
        workbook = RubyXL::Parser.parse file
        sheet    = sheet ? workbook[sheet] : workbook.worksheets.first

        rows = []

        sheet.each do |row|
          next if row.nil?
          rows << row.cells.collect{|c| c.nil? ? nil : c.value}.collect{|c| String === c ? c.gsub("\n", ' ') : c }
        end

        num_values = rows.first.length
        File.open(filename, 'w') do |f|
          if header
            header = rows.shift
            f.puts "#" + header * "\t"
          end

          rows.each do |row| 
            row[num_values-1] ||= nil
            f.puts row * "\t" 
          end
        end

        TSV.open(filename, options)
      end
    end

    def self.write(tsv, file, options = {})
      sheet = Misc.process_options options, :sheet

      fields, rows = TSV._excel_data(tsv, options)

      book = RubyXL::Workbook.new
      sheet1 = book.worksheets.first
      sheet1.sheet_name = sheet if sheet

      fields.each_with_index do |e,i|
        sheet1.add_cell(0, i, e)
      end

      rows.each_with_index do |cells,i|
        cells.each_with_index do |e,j|
          sheet1.add_cell(i+1, j, e)
        end
      end

      book.write file
    end
  end

  def xls(filename, options ={})
    TSV::XLS.write(self, filename, options)
  end

  def xlsx(filename, options ={})
    TSV::XLSX.write(self, filename, options)
  end

  def excel(filename, options ={})
    if filename =~ /\.xlsx$/
      xlsx(filename, options)
    else
      xls(filename, options)
    end
  end


  def self.xls(filename, options ={})
    if Open.remote? filename
      TmpFile.with_file do |tmp|
        Open.download(filename, tmp)
        TSV::XLS.read(tmp, options)
      end
    else
      TSV::XLS.read(filename, options)
    end
  end

  def self.xlsx(filename, options ={})
    if Open.remote? filename

      TmpFile.with_file do |tmp|
        Open.download(filename, tmp)
        TSV::XLSX.read(tmp, options)
      end
    else
      TSV::XLSX.read(filename, options)
    end
  end

  def self.excel(filename, options = {})
    if filename =~ /\.xlsx$/
      xlsx(filename, options)
    else
      xls(filename, options)
    end
  end

  def self.excel2tsv(filename, options ={})
    excel(filename, options)
  end

end
