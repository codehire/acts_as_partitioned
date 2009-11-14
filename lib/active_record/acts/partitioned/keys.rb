module ActiveRecord
  module Acts #:nodoc:
    module Partitioned #:nodoc:
      class Keys < Array
        def columns
          self.map(&:column)
        end

        def column_names
          self.map { |k| k.column_names }.flatten
        end

        # Returns the list of column names excluding this one
        def remaining_columns(column)
          self.column_names - [column]
        end

        def create_partition_tables(model, opts = {})
          each_with_index do |key, index|
            key_opts = index == 0 ? opts : opts.merge(:parent => self[index - 1])
            key.create_partition_table(model, key_opts)
          end
        end

        def partition_handle(opts)
          map { |k| k.partition_handle(opts) }.join("_")
        end
      end
    end
  end
end
