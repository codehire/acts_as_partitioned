module ActiveRecord
  module Acts
    module Partitioned
      class PartitionCacheEntry
        attr_accessor :result

	def match(hash)
	  hash.each_pair do |key, value|
	    unless match_instance(key, value)
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

      class PartitionCacheEntryFactory
	def initialize(keys)
	  @keys = keys
	end

        def create
	  entry = PartitionCacheEntry.new
	  sing = class << entry; self ; end
	  # TODO: Set method to ensure we always provide enough keys when matching
	  @keys.each do |key|
  	    sing.send(:define_method, "#{key.column}=") { |arg|
	      instance_variable_set("@#{key.column}".to_sym, arg)
	    }
	  end
	  entry
        end
      end    

      class PartitionCache
        attr_accessor :result

	def initialize(keys)
	  @pce_fact = PartitionCacheEntryFactory.new(keys)
	  @cache = []
	end

	def add(hash, result)
	  # TODO: Ensure we can't add duplicate keys (maybe just overwrite dupes)
	  @entry = @pce_fact.create
	  @entry.result = result
	  # TODO: Maybe we put this inside the create method
	  hash.each_pair do |key, value|
	    @entry.send("#{key}=", value)
	  end
	  # TODO: Check to see if it has been added
	  @cache << @entry
	  p @cache
	end

	def find(hash)
	  @cache.find do |entry|
	    entry == hash
	  end
	end
      end

      class CopyProxy
	def initialize(keys)
          @keys = keys
	  # Hash with hashes as keys
	  # TODO: Test performance of this
	  @active_partitions = {}
	end

        def <<(hash)
	  values = find_key_values(hash)
	  p = @active_partitions[values]
	  if p
	    puts "Found #{p}"
	  else
	    puts "Need to add"
	    @active_partitions[values] = 1 # TODO: Find the actual partition
	  end
	  # determine partition
	  # grab the partition keys from the hash (raise if missing)
	  # try to find an open copy file
	  # A copy file is linked to a partition - if we don't have one then should we fail or build a new part?
	  # if not create one
	  # expire old copy files
	end

	private
	  def find_key_values(hash)
            values = {}
	    hash.each_pair do |key, value|
	      if @keys.columns.include?(key)
	        values[key] = value
	      end
	    end
	    if values.keys.size < @keys.size
	      raise "Not all keys provided to copy data into partition: #{@keys.columns.join(',')} needed"
	    end
	    values
	  end
      end
    end
  end
end
