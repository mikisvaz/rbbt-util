#{{{ Require files
Rbbt.etc.knowledge_bases.read.split("\n").each do |workflow|
  next if workflow.empty?
  Log.debug("syndicating knowledgebase from workflow #{ workflow }")
  begin
    wf = Kernel.const_get workflow
    begin require "rbbt/knowledge_base/#{ workflow }" rescue Exception end
    KnowledgeBaseRESTHelpers.add_syndication Misc.snake_case(workflow), wf.knowledge_base
  rescue Exception
    Log.warn "Exception loading knowledgebase from #{ workflow }"
    Log.exception $!
  end
end if Rbbt.etc.knowledge_bases.exists?
