require 'set'

module ChainMethods

  def self.chain_methods_extended(base)
    if not base.respond_to? :chain_prefix
      metaclass = class << base
        attr_accessor :chain_prefix, :chained_methods

        def chained_methods
          @chained_methods ||= instance_methods.select{|method| method =~ /^#{chain_prefix}/}
        end
        self
      end

      metaclass.module_eval do
        def setup_chains(base)
          raise "No prefix specified for #{self.to_s}" if self.chain_prefix.nil? or (String === self.chain_prefix and self.chain_prefix.empty?)
          methods = self.chained_methods

          return if methods.empty?

          prefix = self.chain_prefix

          if not base.respond_to?(:processed_chains) or base.processed_chains.nil? or not base.processed_chains.include? prefix
            class << base
              attr_accessor :processed_chains 
            end if not base.respond_to? :processed_chains

            base.processed_chains = Set.new if base.processed_chains.nil?
            base.processed_chains << prefix

            class << base; self; end.module_eval do
              methods.each do |new_method|
                original = new_method.to_s.sub(prefix.to_s + '_', '')
                clean_method = prefix.to_s + '_clean_' + original

                original = "[]" if original == "get_brackets"
                original = "[]=" if original == "set_brackets"

                if base.respond_to? clean_method
                  raise "Method already defined: #{clean_method}. #{ prefix }"
                end

                begin
                  alias_method clean_method, original 
                rescue
                end

                alias_method original, new_method
              end
            end
          end
        end
      end

      if not metaclass.respond_to? :extended
        metaclass.module_eval do
          alias prev_chain_methods_extended extended if methods.include? "extended"

          def extended(base)
            if self.respond_to? :prev_chain_methods_extended 
              prev_chain_methods_extended(base)
            end
            setup_chains(base)
          end
        end
      end
  end

  base.chain_prefix = base.to_s.downcase.to_sym
end

def self.extended(base)
  chain_methods_extended(base)
end
end
