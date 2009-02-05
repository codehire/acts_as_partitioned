module ActiveRecord
  module Acts
    module Partitioned
      module Cache
        class PartitionCache
    	  def initialize(keys)
	    @pce_fact = PartitionCacheEntryFactory.new(keys)
	    @cache = []
	  end

	  def add(partition)
	    # TODO: Ensure we can't add duplicate keys (maybe just overwrite dupes)
	    @entry = @pce_fact.create
	    @entry.partition = partition
	    # TODO: Maybe we put this inside the create method
	    partition.key.each_pair do |key, value|
	      @entry.send("#{key}=", value)
	    end
	    # TODO: Check to see if it has been added
	    @cache << @entry
	  end

	  def find(hash)
	    res = @cache.find do |entry|
	      entry == hash
	    end
            res ? res.partition : nil
	  end
        end
      end
    end
  end
end
