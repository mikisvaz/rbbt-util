class KnowledgeBase
  def syndicate(name, kb)
    kb.all_databases.each do |database|
      if name.nil?
        db_name = database
      else
        db_name = [database, name] * "@"
      end
      file, kb_options = kb.registry[database]
      options = {}
      options[:entity_options] = kb_options[:entity_options]
      options[:undirected] = kb_options[:undirected] if kb_options 
      if kb.entity_options
        options[:entity_options] = kb.entity_options.merge(options[:entity_options] || {})
      end

      register(db_name, nil, options) do
        kb.get_database(database)
      end
    end
  end

  def all_databases
    @registry.keys 
  end
end
