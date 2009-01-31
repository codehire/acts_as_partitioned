module ActiveRecord
  module Acts #:nodoc:
    module Partitioned #:nodoc:
      class Key
        attr_accessor :column, :type, :using

	def initialize(column, options = {})
	  @column = column
	  @type = options[:ranged] ? :continuous : :discrete
	end
      end

      class Keys < Array
        def column_names
          self.inject([]) do |names, key|
            case key.type
              when :continuous
                names << "#{key.column}_begin"
                names << "#{key.column}_end"
              when :discrete
                names << key.column
            end
          end
        end

        # Returns the list of column names excluding this one
        def remaining_columns(column)
          self.column_names - [column]
        end
      end


      # TODO: Rename to Proxy
      # TODO: If we were clever we would merge this with the Partiton AR model
      class Factory
        attr_reader :model, :partition_class
	delegate :find, :to => :partition_class

        def initialize(model, partition_class, options = {})
          @model = model
          spl = @model.table_name.split(".")
          @schema_name, @table_name = if spl.size == 1
            [ nil, spl[0] ]
          else
            spl
          end
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
        def set_validations
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

	def migrate(options = {:force => false})
	  Structure.init_partition_catalog(model, @keys, options)
	end

        # Arguments are the keys specified in creation as a hash
        # eg: create(:date => Date.today, :domain => domain)
        def create(key_hash)
          # TODO: Raise if a key missing
          @model.transaction do
            partition = partition_class.create!(key_hash)
            @model.connection.execute(<<-SQL)
              CREATE TABLE #{table_name}_part_#{partition.id} (
                CHECK (#{apply_check(key_hash).join(' AND ')})
              ) INHERITS (#{table_name});
            SQL
            # TODO: Indexes
            partition
          end
        end


        def determine_column_type(column)
          @model.columns.detect do |c|
            c.name == column.to_s
          end.type
        end

        private
	  def apply_check(key_hash)
  	    checks = []
  	    @keys.each do |key|
	      value = key_hash[key.column]
	      unless value
	        raise "No value provided for key, #{key.column}"
	      end
	      case key.type
	        when :discrete
	          checks << "#{key.column} = '#{value}'"
	        when :continuous
	          checks << "#{key.column} >= '#{value.begin}'"
	          checks << "#{key.column} <#{'=' unless value.exclude_end?} '#{value.begin}'"
	      end
	    end
	    checks
	  end

          def table_name
            if @schema_name
              "#{@schema_name}.#{@table_name}"
            else
              @table_name
            end
          end
      end
    end
  end
end

