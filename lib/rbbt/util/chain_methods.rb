require 'rbbt/util/log'

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
        def setup_chain(object)
          object.extend self
        end

        def setup_chains(base)
          raise "No prefix specified for #{self.to_s}" if self.chain_prefix.nil? or (String === self.chain_prefix and self.chain_prefix.empty?)
          #methods = self.instance_methods.select{|method| method =~ /^#{self.chain_prefix}/}
          methods = self.chained_methods

          return if methods.empty?

          prefix = self.chain_prefix

          new_method = methods.first
          original = new_method.sub(prefix.to_s + '_', '')
          first_clean_method = prefix.to_s + '_clean_' + original

          if not base.respond_to? first_clean_method
            class << base; self; end.module_eval do
              methods.each do |new_method|
                original = new_method.sub(prefix.to_s + '_', '')
                clean_method = prefix.to_s + '_clean_' + original

                original = "[]" if original == "get_brackets"
                original = "[]=" if original == "set_brackets"

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
          alias prev_chain_methods_extended extended

          def extended(base)
            prev_chain_methods_extended(base)
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
