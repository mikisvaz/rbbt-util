$LOAD_PATH.unshift File.join(__dir__)
$LOAD_PATH.unshift File.join(__dir__, '../../lib')

require 'scout/path'
require 'scout/resource'
Path.add_path :rbbt_util, File.join(Path.caller_lib_dir(__FILE__), "{TOPLEVEL}/{SUBPATH}")
module Rbbt
  extend Resource

  self.pkgdir = 'rbbt'
end
Path.default_pkgdir   = Rbbt
Resource.set_software_env Rbbt.software

require_relative 'rbbt/util/version'
require 'scout/log'
require_relative 'rbbt/refactor'

require 'scout/config'
Rbbt::Config = Scout::Config
