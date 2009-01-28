module ActiveRecord
  module Acts #:nodoc:
    module Partitioned #:nodoc:
      class Factory
        attr_reader :model, :partition_class

        # Key can be a symbol or an array of symbols
        # Dates are assumed to be ranges by day
        # Options are:
        #  * rule
        def initialize(model, key, partition_class, options = {})
          @model = model
          spl = @model.table_name.split(".")
          @schema_name, @table_name = if spl.size > 1
            [ nil, spl[0] ]
          else
            spl
          end
          @key = key
          @partition_class = partition_class
          # TODO: Raise if model does not have key column(s)
          @options = options
        end

        # Arguments are the keys specified in creation as a hash
        # eg: create(:date => Date.today, :domain => domain)
        # TODO: Erb?
        def create(key_hash)
          # TODO: Raise if a key missing
          #self.connection.execute(<<-SQL)
          puts(<<-SQL)
            CREATE TABLE #{table_name}_part_#{partition_rule.name(key_hash)} (
              CHECK (#{partition_rule.check(key_hash).join(' AND ')})
            ) INHERITS (#{table_name});
          SQL
          # WeblogPartition.create(key_hash)
          # TODO: Indexes
          # TODO: Return the partition object
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

