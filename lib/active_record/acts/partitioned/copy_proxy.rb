module ActiveRecord
  module Acts
    module Partitioned
      class PartitionCacheEntry
        attr_accessor :partition

        def initialize(keys)
          @keys = keys
          @columns = keys.columns
        end

	def match(hash)
          @columns.each do |column|
            value = hash[column]
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

      class CopyProxy
	def initialize(keys, factory)
          @keys = keys
          @factory = factory
	  @cache = PartitionCache.new(@keys)
	end

	# determine partition
        # grab the partition keys from the hash (raise if missing)
	# try to find an open copy file
	# A copy file is linked to a partition - if we don't have one then should we fail or build a new part?
	# if not create one
	# expire old copy files
        def <<(hash)
	  values = find_key_values(hash)
	  partition = @cache.find(values)
	  unless partition
            # TODO: If there is no partition for then we need to create one
            # We should provide a creation function - specifically how to create a partition with the desired key range
            partition = @factory.find_for(hash)
            raise "No partition for hash (#{hash.inspect})" unless partition
            @cache.add(partition)
	  end
          p @cache
          partition.copy_into << hash
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


      class CopyFile
        # TODO: Make this a configurable option
        ::COPY_FILE_DIRECTORY = "/tmp/dumps/"

        def initialize(table_name, options = {})
          @table_name = table_name
          @options = options
          @header_written = false
          @filename = generate_filename
          @file = File.open(::COPY_FILE_DIRECTORY + @filename, "w")
          #write_meta
        end

        def <<(hash)
          unless @header_written
            write_header(hash.keys)
          end
          # TODO: Write values
          @file << hash.values.map { |v| quote_and_escape(v) }.join(',') << "\n"
        end

        def close
          @file.close
        end

        private
          def quote_and_escape(arg)
            # TODO: Escape - see Adam's cortex code
            "\"#{arg}\""
          end

          def write_meta
            # TODO
          end

          def write_header(keys)
            # TODO
            @file << "COPY #{@table_name} (#{keys.join(',')}) FROM stdin with csv;\n"
            @header_written = true
          end

          def generate_filename
            str = "copy_"
            str << @table_name
            if @options.has_key?(:key)
              str << "_#{@options[:key]}"
            end
            str << tmpstr
          end

          def tmpstr
            str = ""
            8.times do
              str << ((rand * 25).to_i + 97).chr
            end
            str
          end
      end
    end
  end
end
