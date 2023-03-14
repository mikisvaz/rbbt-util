require File.expand_path(File.dirname(__FILE__) + '../../../test_helper')
require 'rbbt/util/tmpfile'
require 'test/unit'
require 'rbbt/knowledge_base'
require 'rbbt/knowledge_base/enrichment'


class TestKnowledgeBaseEnrichment < Test::Unit::TestCase
  def knowledge_base(file)

    effect =StringIO.new <<-END
#: :sep=" "#:type=:double
#SG TG Effect
MDM2 TP53 inhibition
TP53 NFKB1|GLI1 activation|activation true|true
    END

    effect_options = {
      :source => "SG=~Associated Gene Name",
      :target => "TG=~Associated Gene Name=>Ensembl Gene ID",
      :persist => false,
      :identifiers => datafile_test('identifiers'),
      :undirected => true,
      :namespace => "Hsa"
    }

    effect_tsv = TSV.open effect, effect_options.dup 

    knowledge_base = KnowledgeBase.new file, "Hsa/feb2014"
    knowledge_base.format = {"Gene" => "Ensembl Gene ID"}

    knowledge_base.register :effects, effect_tsv, effect_options.dup
    knowledge_base.register :gene_ages, datafile_test('gene_ages')
    knowledge_base.register :nature, datafile_test('nature'), :source => "UniProt/SwissProt Accession", :target => "NCI Nature Pathway ID"

    knowledge_base
  end

  def test_enrichment
    TmpFile.with_file do |tmp_kb|
      genes = %w(P17706-2 LMAN1 P17706-1 P29353-2 JAK3 Q8NFM1 EIF2AK2 JAK1 SRC PIAS1 KPNB1 KPNA2 STAT3 PTPN1 ATR CREBBP PTPRA SGK1 P46108-1 NCK2 O00145 PTPN1 CBL SORBS1 IRS1 SHC1 AKT2 GRB2 SOS1 RPS6KB1 AKT1 DOK1 RASA1 NCK1 FOXO3 RAPGEF1 TRIP10 EIF4EBP1 PDPK1 GRB14 PTPN11 INS GRB10 CAV1 EIF4B RPS6KB1 EEF2 EEF2K AKT1 TSC1 TSC2 CLIP1 DDIT4 SGK1 PDPK1 DEPTOR SREBF1 CYCS IRS1 RPS6KA1 BNIP3 RRN3 RICTOR IKBKB AKT1S1 PXN PML EIF4A1 PPARGC1A YY1 PRKCA RPTOR PDCD4 SIK1 P10636-8 BRSK1 MYC SMARCD3 STK11 ETV4 MARK4 MAP2 MARK2 CRTC2 PSEN2 MST4 CTSD BRSK2 SIK2 ESR1 CAB39 STK11IP SMAD4 CREB1 PRKACA EZR TP53 GSK3B SIK3 CDC37 HSP90AA1 )
      assert knowledge_base(tmp_kb).enrichment(:nature, genes).any?
    end
  end

end


