$LOAD_PATH.unshift File.join(__dir__)
$LOAD_PATH.unshift File.join(__dir__, '../../lib')
require 'scout/path'
require 'scout/log'
require_relative 'rbbt/resource'

module Rbbt
  extend Resource
  self.pkgdir = 'rbbt'
end

require_relative 'rbbt/util/version'
require_relative 'rbbt/refactor'
