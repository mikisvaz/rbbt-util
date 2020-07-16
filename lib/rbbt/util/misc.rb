require 'lockfile'
require 'digest/md5'
require 'cgi'
require 'zlib'
require 'etc'
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

require 'to_regexp'

module MultipleResult; end

module Misc
  def self._convert_match_condition(condition)
    return true if condition == 'true'
    return false if condition == 'false'
    return condition.to_regexp if condition[0] == "/"
    return [:cmp, $1, $2.to_f] if condition =~ /^([<>]=?)(.*)/
    return [:invert, _convert_match_condition(condition[1..-1].strip)] if condition[0] == "!"
    #return {$1 => $2.to_f} if condition =~ /^([<>]=?)(.*)/
    #return {false => _convert_match_condition(condition[1..-1].strip)} if condition[0] == "!"
    return condition
  end

  def self.match_value(value, condition)
    condition = _convert_match_condition(condition.strip) if String === condition

    case condition
    when Regexp
      !! value.match(condition)
    when NilClass, TrueClass
      value === TrueClass or (String === value and value.downcase == 'true')
    when FalseClass
      value === FalseClass or (String === value and value.downcase == 'false')
    when String
      Numeric === value ? value.to_f == condition.to_f : value == condition
    when Numeric
      value.to_f == condition.to_f
    when Array
      case condition.first
      when :cmp
        value.to_f.send(condition[1], condition[2])
      when :invert
        ! match_value(value, condition[1] )
      else
        condition.inject(false){|acc,e| acc = acc ? true : match_value(value, e) }
      end
    else
      raise "Condition not understood: #{Misc.fingerprint condition}"
    end
  end

  def self.tokenize(str)
    str.scan(/"[^"]*"|'[^']*'|[^"'\s]+/)
  end

  def self.timespan(str, default = "s")
    tokens = {
      "s" => (1),
      "sec" => (1),
      "m" => (60),
      "min" => (60),
      "''" => (1),
      "'" => (60),
      "h" => (60 * 60),
      "d" => (60 * 60 * 24),
      "w" => (60 * 60 * 24 * 7),
      "mo" => (60 * 60 * 24 * 30),
      "y" => (60 * 60 * 24 * 365),
    }

    tokens[nil] = tokens[default]
    tokens[""] = tokens[default]
    time = 0
    str.scan(/(\d+)(\w*)/).each do |amount, measure|
      time += amount.to_i * tokens[measure]
    end
    time
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
