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


      # TODO: Rename to Proxy
      class Factory
        attr_reader :model, :partition_class

        # Key can be a symbol or an array of symbols
        # Dates are assumed to be ranges by day
        # Options are:
        #  * rule
        def initialize(model, partition_class, options = {})
          @model = model
          spl = @model.table_name.split(".")
          @schema_name, @table_name = if spl.size == 1
            [ nil, spl[0] ]
          else
            spl
          end
          @keys = []
	  # TODO: Should we raise if we never add any keys?
          @partition_class = partition_class
          # TODO: Raise if model does not have key column(s)
          @options = options
        end

        def partition_by(column, options = {})
	  @keys << Key.new(column, options)
	end

	def migrate(options = {:force => false})
	  Structure.init_partition_catalog(model, @keys, options)
	end

        # Arguments are the keys specified in creation as a hash
        # eg: create(:date => Date.today, :domain => domain)
        # TODO: Erb?
        def create(key_hash)
	  # TODO: Put this in a transaction
          # TODO: Raise if a key missing
          #partiton_id = WeblogPartition.create(key_hash)
          #self.connection.execute(<<-SQL)
	  partition_id = 1 # Remove
          puts(<<-SQL)
            CREATE TABLE #{table_name}_part_#{partition_id} (
              CHECK (#{apply_check(key_hash).join(' AND ')})
            ) INHERITS (#{table_name});
          SQL
          # TODO: Indexes
          # TODO: Return the partition object
        end

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

        def find(arg)
          if arg == :all
            puts "All"
            conditions = "tablename LIKE '#{@table_name}_part_%'"
            conditions << if @schema_name
              " AND schemaname = '#{@schema_name}'"
            else
              " AND schemaname = current_schema()"
            end
            parts = self.find(:all, :from => "pg_catalog.pg_tables", :conditions => conditions)
            # TODO: Convert to partition objects
          else
            puts partition_rule.name(arg)
          end
        end

        def determine_column_type(column)
          @model.columns.detect do |c|
            c.name == column.to_s
          end.type
        end

        private
          def partition_rule
            if @options.has_key?(:rule)
              rule = @options[:rule]
              case rule
                when Symbol
                  rule.to_s.constantize.new(self)
                when Class
                  rule.new(self)
              end
            else
              ByValue.new(self)
            end
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

