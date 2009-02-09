module ActiveRecord
  module Acts
    module Partitioned
      module Cache
        class PartitionCache
    	  def initialize(keys)
	    @pce_fact = PartitionCacheEntryFactory.new(keys)
	    @cache = []
	  end

          # TODO: Add cache expiry
	  def add(partition)
	    @entry = @pce_fact.create
	    @entry.partition = partition
	    partition.key.each_pair do |key, value|
	      @entry.send("#{key}=", value)
	    end
	    found = self.find(partition.key)
	    @cache << @entry unless found
	  end

	  def find(hash)
	    res = @cache.find do |entry|
	      entry == hash
	    end
            res ? res.partition : nil
	  end

          def size
            @cache.size
          end
        end
      end
    end
  end
end
