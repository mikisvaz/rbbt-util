require 'rbbt/util/pkg_config'

module Rbbt
  CFG_VARIABLES = %w(tmpdir cachedir datadir)
  include PKGConfig
end
