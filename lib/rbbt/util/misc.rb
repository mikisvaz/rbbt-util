require_relative '../../rbbt'
require 'digest/md5'
require 'cgi'
require 'zlib'
require 'etc'
require 'rubygems/package'

require_relative 'tar'
require_relative 'misc/exceptions'
require_relative 'misc/concurrent_stream'
require_relative 'misc/indiferent_hash'
require_relative 'misc/pipes'
require_relative 'misc/format'
require_relative 'misc/omics'
require_relative 'misc/inspect'
#require_relative 'misc/math'
require_relative 'misc/development'
require_relative 'misc/lock'
require_relative 'misc/options'
require_relative 'misc/system'
require_relative 'misc/objects'
require_relative 'misc/manipulation'
require_relative 'misc/communication'

require_relative 'misc/serialize'

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
    str.scan(/"([^"]*)"|'([^']*)'|([^"'\s]+)/).flatten.compact
  end

  def self.timespan(str, default = "s")

    return - timespan(str[1..-1], default) if str[0] == "-"
    
    if str.include?(":")
      seconds, minutes, hours = str.split(":").reverse
      return seconds.to_i + minutes.to_i * 60 + hours.to_i * 60 * 60
    end

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
      "mo" => (60 * 60 * 24 * 31),
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

  def self.name2basename(file)
    sanitize_filename(file.gsub("/",'·').gsub("~", '-'))
  end

  def self.sanitize_filename(filename, length = 254)
    if filename.length > length
      if filename =~ /(\..{2,9})$/
        extension = $1
      else
        extension = ''
      end

      post_fix = "--#{filename.length}@#{length}_#{Misc.digest(filename)[0..4]}" + extension

      filename = filename[0..(length - post_fix.length - 1)] << post_fix
    else
      filename
    end
    filename
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
