module SOPT
  def self.name(info)
    (info[:long] || info[:short]).sub(/^-*/,'')
  end

  def self.parse(opts)
    info = {}
    opts.split(/:/).each do |opt|
      short, long = opt.sub(/\*$/,'').split('--').values_at(0,1)
      i= {
        :arg => !opt.match(/\*$/).nil?,
      }
      i[:short] = short unless short.nil? || short.empty?
      i[:long] = '--' + long unless long.nil? || long.empty?
      info[name(i)] = i
    end

    info
  end

  def self.get(opts)
    info = parse(opts)

    switches = {}
    info.each do |name, i|
      switches[i[:short]] = name if i[:short]
      switches[i[:long]] = name if i[:long]
    end

    options = Hash.new(false)
    rest = []

    index = 0
    while index < ARGV.length do
      arg = ARGV[index]
      if switches.include? arg
        name = switches[arg]
        i = info[name]
        if i[:arg]
          options[name.to_sym] = ARGV[index + 1]
          index += 1
        else
          options[name.to_sym] = true
        end
      else
        rest << arg
      end
      index += 1
    end

    ARGV.delete_if do true end
    rest.each do |e| ARGV << e end

    options
  end
end
