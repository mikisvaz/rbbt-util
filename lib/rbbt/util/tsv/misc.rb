require 'rbbt/util/tsv'

class TSV
  def self.keys(file, sep = "\t")
    CMD.cmd("cut -f 1 -d'#{sep}' '#{file}'|grep -v ^#").read.split("\n")
  end

  def self.field_match_counts(file, values)
    key_field, fields = TSV.parse_header(Open.open(file))
    fields.unshift key_field

    counts = {}
    TmpFile.with_file do |tmpfile|
      if Array === values
        Open.write(tmpfile, values * "\n")
      else
        FileUtils.ln_s values, tmpfile
      end

      fields.each_with_index do |field,i|
        counts[field] = begin
                          CMD.cmd("cat #{ file } |grep -v ^#|cut -f #{i + 1}|tr '|' '\\n' |sort -u |grep [[:alpha:]]|grep -f #{tmpfile} -F -w").read.count("\n")
                        rescue
                          0
                        end
      end
    end

    counts
  end
end
