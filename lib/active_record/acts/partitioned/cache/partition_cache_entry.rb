module ActiveRecord
  module Acts
    module Partitioned
      module Cache
        class PartitionCacheEntry
          attr_accessor :partition

          def initialize(keys)
            @keys = keys
            @columns = keys.columns
          end

    	  def match(hash)
            @columns.each do |column|
              value = hash[column.to_sym]
              raise "No value provided for #{column}" unless value
	      unless match_instance(column, value)
	        return false
	      end
	    end
	    true
	  end
	  alias :== :match

  	  private
	    def match_instance(key, value)
	      instance_variable_get("@#{key}") === value
	    end
        end
      end
    end
  end
end
