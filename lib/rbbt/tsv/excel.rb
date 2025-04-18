Rbbt.require_instead 'scout/tsv'
require_relative '../refactor'
require 'spreadsheet'
require 'rubyXL'

module TSV

  def self._remove_link(value)
    if String === value && value =~ /<([\w]+)[^>]*>(.*?)<\/\1>/
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
    options = IndiferentHash.add_defaults options, :sep2 => ', '

    name = IndiferentHash.process_options options, :name
    sep2 = IndiferentHash.process_options options, :sep2
    unmerge = IndiferentHash.process_options options, :unmerge
    sort_by = IndiferentHash.process_options options, :sort_by
    sort_by_cast = IndiferentHash.process_options options, :sort_by_cast
    remove_links = IndiferentHash.process_options options, :remove_links

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
        NamedArray.zip_fields(row).each do |values|
          new_rows << [header] + values
        end
      end
      rows = new_rows
    end
    [tsv.all_fields, rows]
  end

  module XLS
    def self.read(file, options = {})
      options = IndiferentHash.add_defaults options, :sep2 => /[,|]\s?/, :skip_rows => 0
      sheet = IndiferentHash.process_options options, :sheet
      header = IndiferentHash.process_options options, :header
      text = IndiferentHash.process_options options, :text
      skip_rows = IndiferentHash.process_options options, :skip_rows
      skip_rows = skip_rows.to_i

      header = true unless header == false
      sheet ||= "0"

      workbook = Spreadsheet.open Open.open(file)

      if sheet && sheet.to_s =~ /^\d+$/
        sheet = workbook.worksheets.collect{|s| s.name }[sheet.to_i]
      end
      sheet_name = sheet
      Log.debug "Opening LSX #{file} sheet #{ sheet_name }"

      TmpFile.with_file :extension => Path.sanitize_filename(sheet_name.to_s) do |filename|

        sheet    = workbook.worksheet sheet

        rows = []

        sheet.each do |row|
          if skip_rows > 0
            skip_rows -= 1
            next
          end
          
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

        text ? Open.read(filename) : TSV.open(filename, options)
      end
    end

    def self.write(tsv, file, options = {})
      options = IndiferentHash.add_defaults options, :sheet => "Sheet1"
      sheet = IndiferentHash.process_options options, :sheet
      fields, rows = TSV._excel_data(tsv, options)

      book = Spreadsheet::Workbook.new
      sheet1 = book.create_worksheet 
      sheet1.name = sheet if sheet

      if fields
        sheet1.row(0).concat fields if fields
      else
        sheet1.row(0).concat ["No field info"]
      end

      rows.each_with_index do |cells,i|
        sheet1.row(i+1).concat cells
      end

      book.write file
    end
  end

  module XLSX
    def self.read(file, options = {})
      options = IndiferentHash.add_defaults options, :sep2 => /[,|]\s?/, :skip_rows => 0
      sheet = IndiferentHash.process_options options, :sheet
      header = IndiferentHash.process_options options, :header
      text = IndiferentHash.process_options options, :text
      skip_rows = IndiferentHash.process_options options, :skip_rows
      skip_rows = skip_rows.to_i

      header = true unless header == false

      sheet ||= "0"
      workbook = RubyXL::Parser.parse file
      if sheet && sheet =~ /^\d+$/
        sheet = workbook.worksheets.collect{|s| s.sheet_name }[sheet.to_i]
      end
      sheet_name = sheet
      Log.debug "Opening XLSX #{file} sheet #{ sheet_name }"

      TmpFile.with_file :extension => Path.sanitize_filename(sheet_name.to_s) do |filename|

        sheet    = sheet_name ? workbook[sheet_name] : workbook.worksheets.first

        raise "No sheet #{sheet_name} found" if sheet.nil?

        rows = []

        sheet.each do |row|
          next if row.nil?
          if skip_rows > 0
            skip_rows -= 1
            next
          end
          
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

        text ? Open.read(filename) : TSV.open(filename, options)
      end
    end

    def self.write(tsv, file, options = {})
      sheet, add_sheet = IndiferentHash.process_options options, :sheet, :add_sheet

      fields, rows = TSV._excel_data(tsv, options)

      if Open.exists?(file) && add_sheet
        book = RubyXL::Parser.parse file
        sheet1 = book.add_worksheet(sheet)
      else
        book = RubyXL::Workbook.new
        sheet1 = book.worksheets.first
        sheet1.sheet_name = sheet if sheet
      end

      fields.each_with_index do |e,i|
        sheet1.add_cell(0, i, e)
      end if fields

      rows.each_with_index do |cells,i|
        i += 1 if fields
        cells.each_with_index do |e,j|
          sheet1.add_cell(i, j, e)
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
      TmpFile.with_file nil, :extension => 'xls' do |tmp|
        Open.download(filename, tmp)
        TSV::XLS.read(tmp, options)
      end
    else
      TSV::XLS.read(filename, options)
    end
  end

  def self.xlsx(filename, options ={})
    if Open.remote? filename

      TmpFile.with_file nil, :extension => 'xlsx' do |tmp|
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
