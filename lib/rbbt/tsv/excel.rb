require 'spreadsheet'
module TSV
  def self.excel(tsv, filename, options ={})
    name = Misc.process_options options, :name
    sort_by = Misc.process_options options, :sort_by
    sort_by_cast = Misc.process_options options, :sort_by_cast
    fields = Misc.process_options(options, :fields) || all_fields

    book = Spreadsheet::Workbook.new
    sheet1 = book.create_worksheet 
    sheet1.row(0).concat fields
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

    data.each do |key, values|
      cells = []
      cells.push((name and key.respond_to?(:name)) ?  key.name || key : key )

      values.each do |value|
        v = (name and value.respond_to?(:name)) ?  value.name || value : value 
        if Array === v
          cells.push v * ", "
        else
          cells.push v
        end
      end

      sheet1.row(i).concat cells
      i += 1
    end
    book.write filename
  end

  def excel(filename, options ={})
    name = Misc.process_options options, :name
    sort_by = Misc.process_options options, :sort_by
    sort_by_cast = Misc.process_options options, :sort_by_cast

    book = Spreadsheet::Workbook.new
    sheet1 = book.create_worksheet 
    sheet1.row(0).concat all_fields
    i = 1
    if sort_by
      if sort_by_cast
        data = self.sort_by sort_by do |k, v| 
          if Array === v
            v.first.send(sort_by_cast)
          else
            v.send(sort_by_cast)
          end
        end
      else
        data = self.sort_by sort_by
      end
    else
      data = self
    end

    data.each do |key, values|
      cells = []
      cells.push((name and key.respond_to?(:name)) ?  key.name || key : key )

      values.each do |value|
        v = (name and value.respond_to?(:name)) ?  value.name || value : value 
        if Array === v
          cells.push v * ", "
        else
          cells.push v
        end
      end

      sheet1.row(i).concat cells
      i += 1
    end
    book.write filename
  end
end
