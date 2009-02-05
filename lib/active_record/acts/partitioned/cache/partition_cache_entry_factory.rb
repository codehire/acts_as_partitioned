module ActiveRecord
  module Acts
    module Partitioned
      module Cache
        class PartitionCacheEntryFactory
	  def initialize(keys)
	    @keys = keys
	  end

          def create
	    entry = PartitionCacheEntry.new(@keys)
	    sing = class << entry; self ; end
	    @keys.each do |key|
  	      sing.send(:define_method, "#{key.column}=") { |arg|
	        instance_variable_set("@#{key.column}".to_sym, arg)
	      }
	    end
	    entry
          end
        end    
      end
    end
  end
end
