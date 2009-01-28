module ActiveRecord
  module Acts
    module Partitioned
      class Structure < ActiveRecord::Migration
        def self.init_partition_catalog(model, keys, options = {})
          create_table("#{model.table_name}_partitions", :force => options[:force]) do |t|
            t.string :name
            puts "keys are '#{keys.inspect}'"
            ref = model.table_name
	    keys.each do |key|
	      case key.type
	        when :discrete
                  t.column "#{ref}_#{key.column}", determine_column_type(model, key.column)
		when :continuous
                  t.column "#{ref}_#{key.column}_begin", determine_column_type(model, key.column)
                  t.column "#{ref}_#{key.column}_end", determine_column_type(model, key.column)
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
