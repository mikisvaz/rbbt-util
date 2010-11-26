require 'rbbt/util/pkg_config'
require 'rbbt/util/pkg_data'
require 'rbbt/util/open'
require 'rbbt/util/tmpfile'
require 'rbbt/util/filecache'

module Rbbt
  extend PKGConfig
  extend PKGData

  self.load_cfg(%w(tmpdir cachedir datadir))
end

