module Misc

  IUPAC2BASE = {
    "A" => ["A"],
    "C" => ["C"],
    "G" => ["G"],
    "T" => ["T"],
    "U" => ["U"],
    "R" => "A or G".split(" or "),
    "Y" => "C or T".split(" or "),
    "S" => "G or C".split(" or "),
    "W" => "A or T".split(" or "),
    "K" => "G or T".split(" or "),
    "M" => "A or C".split(" or "),
    "B" => "C or G or T".split(" or "),
    "D" => "A or G or T".split(" or "),
    "H" => "A or C or T".split(" or "),
    "V" => "A or C or G".split(" or "),
    "N" => %w(A C T G),
  }

  BASE2COMPLEMENT = {
    "A" => "T",
    "C" => "G",
    "G" => "C",
    "T" => "A",
    "U" => "A",
  }

  THREE_TO_ONE_AA_CODE = {
    "ala" =>   "A",
    "arg" =>   "R",
    "asn" =>   "N",
    "asp" =>   "D",
    "cys" =>   "C",
    "glu" =>   "E",
    "gln" =>   "Q",
    "gly" =>   "G",
    "his" =>   "H",
    "ile" =>   "I",
    "leu" =>   "L",
    "lys" =>   "K",
    "met" =>   "M",
    "phe" =>   "F",
    "pro" =>   "P",
    "ser" =>   "S",
    "thr" =>   "T",
    "trp" =>   "W",
    "tyr" =>   "Y",
    "val" =>   "V"
  }
  CODON_TABLE = {
    "ATT" => "I",
    "ATC" => "I",
    "ATA" => "I",
    "CTT" => "L",
    "CTC" => "L",
    "CTA" => "L",
    "CTG" => "L",
    "TTA" => "L",
    "TTG" => "L",
    "GTT" => "V",
    "GTC" => "V",
    "GTA" => "V",
    "GTG" => "V",
    "TTT" => "F",
    "TTC" => "F",
    "ATG" => "M",
    "TGT" => "C",
    "TGC" => "C",
    "GCT" => "A",
    "GCC" => "A",
    "GCA" => "A",
    "GCG" => "A",
    "GGT" => "G",
    "GGC" => "G",
    "GGA" => "G",
    "GGG" => "G",
    "CCT" => "P",
    "CCC" => "P",
    "CCA" => "P",
    "CCG" => "P",
    "ACT" => "T",
    "ACC" => "T",
    "ACA" => "T",
    "ACG" => "T",
    "TCT" => "S",
    "TCC" => "S",
    "TCA" => "S",
    "TCG" => "S",
    "AGT" => "S",
    "AGC" => "S",
    "TAT" => "Y",
    "TAC" => "Y",
    "TGG" => "W",
    "CAA" => "Q",
    "CAG" => "Q",
    "AAT" => "N",
    "AAC" => "N",
    "CAT" => "H",
    "CAC" => "H",
    "GAA" => "E",
    "GAG" => "E",
    "GAT" => "D",
    "GAC" => "D",
    "AAA" => "K",
    "AAG" => "K",
    "CGT" => "R",
    "CGC" => "R",
    "CGA" => "R",
    "CGG" => "R",
    "AGA" => "R",
    "AGG" => "R",
    "TAA" => "*",
    "TAG" => "*",
    "TGA" => "*",
  }

  def self.correct_icgc_mutation(pos, ref, mut_str)
    mut = mut_str
    mut = '-' * (mut_str.length - 1) if mut =~/^-[ACGT]/
      mut = "+" << mut if ref == '-'
    [pos, [mut]]
  end

  def self.correct_mutation(pos, ref, mut_str)
    muts = mut_str.nil? ? [] : mut_str.split(',')
    muts.collect!{|m| m == '<DEL>' ? '-' : m }

    ref = '' if ref == '-'
    while ref.length >= 1 and muts.reject{|m| m[0] == ref[0]}.empty?
      ref = ref[1..-1]
      raise "REF nil" if ref.nil?
      pos = pos + 1
      muts = muts.collect{|m| m[1..-1]}
    end

    muts = muts.collect do |m|
      m = '' if m == '-'
      case
      when ref.empty?
        "+" << m
      when (m.length < ref.length and (m.empty? or ref.index(m)))
        "-" * (ref.length - m.length)
      when (ref.length == 1 and m.length == 1)
        m
      else
        Log.debug{"Cannot understand: #{[ref, m]} (#{ muts })"}
        '-' * ref.length + m
      end
    end

    [pos, muts]
  end

  def self.correct_vcf_mutation(pos, ref, mut_str)
    muts = mut_str.nil? ? [] : mut_str.split(',')
    muts.collect!{|m| m == '<DEL>' ? '-' : m }

    while ref.length >= 1 and muts.reject{|m| m[0] == ref[0]}.empty?
      ref = ref[1..-1]
      raise "REF nil" if ref.nil?
      pos = pos + 1
      muts = muts.collect{|m| m[1..-1]}
    end

    muts = muts.collect do |m|
      case
      when ref.empty?
        "+" << m
      when (m.length < ref.length and (m.empty? or ref.index(m)))
        "-" * (ref.length - m.length)
      when (ref.length == 1 and m.length == 1)
        m
      else
        Log.debug{"Cannot understand: #{[ref, m]} (#{ muts })"}
        '-' * ref.length + m
      end
    end

    [pos, muts]
  end

  def self.IUPAC_to_base(iupac)
    IUPAC2BASE[iupac]
  end


  def self.sort_mutations(mutations)
    mutations.collect do |mutation|
      chr,pos,mut = mutation.split ":"
      chr.sub!(/^chr/i,'')
      chr = 22 if chr == "Y"
      chr = 23 if chr == "X"
      chr = 24 if chr == "MT" or chr == "M"
      [chr.to_i, pos.to_i, mut, mutation]
    end.sort do |a,b|
      case a[0] <=> b[0]
      when -1
        -1
      when 1
        1
      when 0
        case a[1] <=> b[1]
        when -1
          -1
        when 1
          1
        when 0
          a[2] <=> b[2]
        end
      end
    end.collect{|p| p.last }
  end

  def self.ensembl_server(organism)
    date = organism.split("/")[1]
    if date.nil?
      "www.ensembl.org"
    else
      "#{ date }.archive.ensembl.org"
    end
  end


  def self.intersect_streams_read(io, sep=":")
    line = io.gets.strip
    parts = line.split(sep)
    chr, start, eend, *rest = parts
    start = start.to_i
    if eend =~ /^\d+$/
      eend = eend.to_i
    else
      eend = start.to_i
    end
    [line,chr, start, eend, rest]
  end

  def self.intersect_streams_cmp_chr(chr1, chr2)
    if chr1 =~ /^\d+$/ and chr2 =~ /^\d+$/
      chr1 <=> chr2
    elsif chr1 =~ /^\d+$/
      -1
    elsif chr2 =~ /^\d+$/
      1
    else
      chr1 <=> chr2
    end
  end

  def self.intersect_streams(f1, f2, out, sep=":")
    finish = false
    return if f1.eof? or f2.eof?
    line1, chr1, start1, eend1, rest1 = intersect_streams_read(f1,sep)
    line2, chr2, start2, eend2, rest2 = intersect_streams_read(f2,sep)
    while not finish
      cmp = intersect_streams_cmp_chr(chr1,chr2)
      case cmp
      when -1
        move = 1
      when 1
        move = 2
      else
        if eend1 < start2
          move = 1
        elsif eend2 < start1
          move = 2
        else
          pos2 = f2.pos

          sline2, schr2, sstart2, seend2, srest2 = line2, chr2, start2, eend2, rest2
          while chr1 == chr2 and ((start1 <= eend2 and eend1 >= start2)) 
            out.puts line1 + "\t" + line2
            if f2.eof?
              chr2 = 'next2'
            else
              line2, chr2, start2, eend2, rest2 = intersect_streams_read(f2,sep)
            end
          end
          line2, chr2, start2, eend2, rest2 = sline2, schr2, sstart2, seend2, srest2 
          f2.seek(pos2)
          move = 1
        end
      end

      case move
      when 1
        if f1.eof?
          finish = true
        else
          line1, chr1, start1, eend1, rest1 = intersect_streams_read(f1,sep)
        end
      when 2
        if f2.eof?
          finish = true
        else
          line2, chr2, start2, eend2, rest2 = intersect_streams_read(f2,sep)
        end
      end
    end
  end

  def self.select_ranges(stream1, stream2, sep = "\t")
    Misc.open_pipe do |sin|
      intersect_streams(stream1, stream2,sin, sep)
    end
  end
end
