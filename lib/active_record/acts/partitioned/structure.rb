module ActiveRecord
  module Acts
    module Partitioned
      class Structure < ActiveRecord::Migration
        def self.init_partition_catalog(model, keys, options = {})
          create_table("#{model.table_name.singularize}_partitions", :force => options[:force]) do |t|
            puts "keys are '#{keys.inspect}'"
	    keys.each do |key|
	      case key.type
	        when :discrete
                  t.column key.column, determine_column_type(model, key.column)
		when :continuous
                  t.column "#{key.column}_begin", determine_column_type(model, key.column)
                  t.column "#{key.column}_end", determine_column_type(model, key.column)
                  t.column "#{key.column}_exclusive", :boolean
              end
	    end
            # TODO: Add key columns and indexes
          end
        end

        def self.determine_column_type(model, column)
	  # TODO: Raise if the column does not exist
          model.columns.detect do |c|
            c.name == column.to_s
          end.type
        end
      end
    end
  end
end
