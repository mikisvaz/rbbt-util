require 'rbbt/util/tc_hash'

module Stage
  class << self
    attr_accessor :basedir
  end

  def self.name(stage, job, options)
    base = basedir || '.'
    File.join(base, stage.to_s, job.to_s + '_' << Misc.hash2md5(options))
  end
end
