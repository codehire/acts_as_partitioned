module ActiveRecord
  module Acts
    module Partitioned
      class Structure < ActiveRecord::Migration
        def self.init_partition_catalog(model, key, options = {})
          create_table("#{model.table_name}_partitions", :force => options[:force]) do |t|
            t.string :name
            puts "keys are '#{key.inspect}'"
            ref = model.table_name
            case key
              when Symbol, String
                t.column "#{ref}_#{key}", determine_column_type(model, key)
              when Array
                key.each do |k|
                  t.column "#{ref}_#{k}", determine_column_type(model, k)
                end
            end
            # TODO: Add key columns and indexes
          end
        end

        def self.determine_column_type(model, column)
          model.columns.detect do |c|
            c.name == column.to_s
          end.type
        end
      end
    end
  end
end
