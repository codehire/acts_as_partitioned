module ActiveRecord
  module Acts #:nodoc:
    module Partitioned #:nodoc:

      # TODO: Rename to Proxy
      # TODO: If we were clever we would merge this with the Partiton AR model - can't merge as you need a proxy instance but we can move lots of methods over
      class Factory
        attr_reader :model, :partition_class
        attr_reader :keys
      	delegate :find, :to => :partition_class
      	delegate :count, :to => :partition_class
	      delegate :with_key, :to => :partition_class

        def initialize(model, partition_class, options = {})
          @model = model
          @keys = Keys.new
      	  # TODO: Should we raise if we never add any keys?
          @partition_class = partition_class
          partition_class.set_factory(self)
          # TODO: Raise if model does not have key column(s)
          @options = options
        end

        def partition_by(column, options = {})
          # TODO: Raise if caller tries to partition on primary key
          @keys << Key.new(column, options)
        end

        # TODO: Prevent overlapping ranges
      	# TODO: Private?
        def set_validations
          # TODO: Move below this line to the partition class itself
          @keys.each do |key|
            case key.type
              when :continuous
                partition_class.validates_uniqueness_of("#{key.column}_begin", :scope => @keys.remaining_columns("#{key.column}_begin"))
                partition_class.validates_uniqueness_of("#{key.column}_end", :scope => @keys.remaining_columns("#{key.column}_end"))
              when :discrete
                partition_class.validates_uniqueness_of(key.column, :scope => @keys.remaining_columns(key.column))
            end
          end
        end

        def init(options = {:force => false})
          Structure.init_partition_catalog(model, @keys, options)
        end

        def copy(filename, db_name = nil)
          port, host, user, db = if db_name
            config = ActiveRecord::Base.configurations[db_name.to_s]
            raise "No such DB configuration: #{db_name}" unless config
            [config['port'], config['host'], config['username'], config['database']]
          else
            conn = @model.connection.raw_connection
            [conn.port, conn.host, conn.user, conn.db]
          end
          "psql --set ON_ERROR_STOP=1 --single-transaction -p #{port} -h #{host} -U #{user} #{db} < #{filename}"
        end

        # Arguments are the keys specified in creation as a hash
        # eg: create(:date => Date.today, :domain => domain)
        def create(key_hash)
          # TODO: Raise if a key missing
          @model.transaction do
            partition = partition_class.create!(key_hash)
            @keys.create_partition_tables(@model, :key_hash => key_hash)
            # TODO: Indexes
            partition
          end
        end

        def clear
          partition_class.find(:all).each do |partition|
            partition.drop!
          end
        end

        # Finds a partition to which these keys belong
        # Not by keys used to create the partition
        # This is the same thing for discrete keys
        # but for continuous (ie; ranged keys)
        # the end points of a range may not equal the values
        # stored in the part
        # Here we see if a value fits within the range
        # Use this method if you want to know which partition
        # to write data to
        def find_for(_hash)
          hash = _hash.symbolize_keys
          conditions = {}
          @keys.each do |key|
            puts "key = #{key.inspect}"
            value = hash[key.column.to_sym]
            raise "No value provided for #{key.column}" unless value
            case key.type
              when :discrete
                conditions[key.column.to_sym] = value
              when :continuous
                conditions[:"#{key.column}_begin"] = value.begin
                conditions[:"#{key.column}_end"] = value.end
                conditions[:"#{key.column}_exclusive"] = value.exclude_end?
            end
          end
          puts "conditions = #{conditions.inspect}"
          partition_class.find(:first, :conditions => conditions)
        end

        def find_or_create_for(hash)
          find_for(hash) || create(hash)
        end

      	def dump_age
          if @options[:dump_age].kind_of?(Proc)
            @options[:dump_age].call || 0
          else
            @options[:dump_age] || 0
          end
	      end

	      def archive?
          @options[:archive] || false
	      end
      end
    end
  end
end

