require 'lockfile'
require 'net/smtp'
require 'digest/md5'
require 'cgi'
require 'zlib'
require 'rubygems/package'
require 'rbbt/util/tar'
require 'rbbt/util/misc/exceptions'
require 'rbbt/util/misc/concurrent_stream'
require 'rbbt/util/misc/indiferent_hash'
require 'rbbt/util/misc/pipes'
require 'rbbt/util/misc/format'
require 'rbbt/util/misc/omics'
require 'rbbt/util/misc/inspect'
require 'rbbt/util/misc/math'
require 'rbbt/util/misc/development'
require 'rbbt/util/misc/lock'
require 'rbbt/util/misc/options'
require 'rbbt/util/misc/system'
require 'rbbt/util/misc/objects'
require 'rbbt/util/misc/manipulation'

module Misc
  def self.ensembl_server(organism)
    date = organism.split("/")[1]
    if date.nil?
      "www.ensembl.org"
    else
      "#{ date }.archive.ensembl.org"
    end
  end


  def self.google_venn(list1, list2, list3, name1 = nil, name2 = nil, name3 = nil, total = nil)
    name1 ||= "list 1"
    name2 ||= "list 2"
    name3 ||= "list 3"

    sizes = [list1, list2, list3, list1 & list2, list1 & list3, list2 & list3, list1 & list2 & list3].collect{|l| l.length}

    total = total.length if Array === total

    label = "#{name1}: #{sizes[0]} (#{name2}: #{sizes[3]}, #{name3}: #{sizes[4]})"
    label << "|#{name2}: #{sizes[1]} (#{name1}: #{sizes[3]}, #{name3}: #{sizes[5]})"
      label << "|#{name3}: #{sizes[2]} (#{name1}: #{sizes[4]}, #{name2}: #{sizes[5]})"
      if total
        label << "| INTERSECTION: #{sizes[6]} TOTAL: #{total}"
      else
        label << "| INTERSECTION: #{sizes[6]}"
      end

    max = total || sizes.max
    sizes = sizes.collect{|v| (v.to_f/max * 100).to_i.to_f / 100}
    url = "https://chart.googleapis.com/chart?cht=v&chs=500x300&chd=t:#{sizes * ","}&chco=FF6342,ADDE63,63C6DE,FFFFFF&chdl=#{label}"
  end

end

module PDF2Text
  def self.pdftotext(filename, options = {})
    require 'rbbt/util/cmd'
    require 'rbbt/util/tmpfile'
    require 'rbbt/util/open'


    TmpFile.with_file(Open.open(filename, options.merge(:nocache => true)).read) do |pdf_file|
      CMD.cmd("pdftotext #{pdf_file} -", :pipe => false, :stderr => true)
    end
  end
end
