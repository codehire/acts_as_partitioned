module ActiveRecord
  module Acts #:nodoc:
    module Partitioned #:nodoc:
      class Key
        attr_accessor :column, :type, :using

        def initialize(column, options = {})
          @column = column.to_s
          @type = options[:ranged] ? :continuous : :discrete
        end

        def column_names
          case @type
            when :continuous then ["#{@column}_begin", "#{@column}_end"]
            when :discrete then [@column]
          end
        end

        def create_partition_table(model, opts = {})
          table_name = model.table_name
          table_name << "_part_" + opts[:parent].partition_handle(:key_hash => opts[:key_hash]) if opts.has_key?(:parent)
          partition_name = "#{model.table_name}_part_#{partition_handle(opts)}"
          unless model.connection.tables.include?(partition_name)
            model.connection.execute(<<-SQL)
              CREATE TABLE #{partition_name} (
                CHECK (#{apply_check(opts[:key_hash]).join(' AND ')})
              ) INHERITS (#{table_name});
            SQL
          end
        end

        def partition_handle(opts)
          handle = []
          handle << opts[:parent].partition_handle(:key_hash => opts[:key_hash]) if opts.has_key?(:parent)
          value = opts[:key_hash][@column.to_sym]
          handle << case @type
            when :discrete then value
            when :continuous then [ value.begin, value.end ]
          end
          handle.flatten.map do |h|
            case h
              when Date,Time,Timestamp then h.strftime("%Y%m%d%H%M")
              else h
            end
          end.join("_")
        end

        private
	        def apply_check(key_hash)
            value = key_hash[@column.to_sym]
            unless value
              raise "No value provided for key #{@column}, hash is #{key_hash.inspect}"
            end
            case @type
              when :discrete then ["#{@column} = '#{value}'"]
              when :continuous then ["#{@column} >= '#{value.begin}'", "#{@column} <#{'=' unless value.exclude_end?} '#{value.end}'"]
            end
          end
      end
    end
  end
end
