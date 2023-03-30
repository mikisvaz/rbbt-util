require 'scout/path'
require 'scout/log'
require_relative 'rbbt/resource'

module Rbbt
  extend Resource
  pkgdir = 'rbbt'
  libdir = Path.caller_lib_dir
end

require_relative 'rbbt/util/version'
require_relative 'rbbt/refactor'
