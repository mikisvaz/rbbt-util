$LOAD_PATH.unshift File.join(__dir__)
$LOAD_PATH.unshift File.join(__dir__, '../../lib')
require 'scout/path'
require 'scout/log'
require 'scout/resource'
require_relative 'rbbt/resource'
require_relative 'rbbt/util/version'
require_relative 'rbbt/refactor'

module Rbbt
  extend Resource

  self.pkgdir = 'rbbt'
end

Resource.set_software_env Rbbt.software

Scout = Rbbt

#Path.path_maps[:rbbt_util] = File.join(Path.caller_lib_dir(__FILE__), "{TOPLEVEL}/{SUBPATH}")
