module TSV
  annotation :entity_options, :entity_templates

  def entity_options
    @entity_options ||= {}
  end

  def entity_templates
    @entity_templates ||= {}
  end
end
