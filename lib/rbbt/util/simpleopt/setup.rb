require 'rbbt/util/simpleopt/parse'
require 'rbbt/util/simpleopt/get'
require 'rbbt/util/simpleopt/doc'
module SOPT
  def self.setup(str)
    summary, synopsys, description, options = str.split(/\n\n+/)

    if summary[0]=="-"
      summary, synopsys, description, options = nil, nil, nil, summary
    end

    if synopsys and synopsys[0] != "$"
      description, options = synopsys, description
      synopsys = nil
    end

    if description and description[0] == "-"
      description, options = nil, description
    end

    synopsys.sub!(/^\$\s+/,'') if synopsys

    SOPT.summary = summary.strip if summary
    SOPT.synopsys = synopsys.strip if synopsys
    SOPT.description = description.strip if description
    SOPT.parse options  if options

    SOPT.consume
  end
end
