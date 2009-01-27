module ActiveRecord
  module Acts #:nodoc:
    module Partitioned #:nodoc:
      # Values must be ranges
      class ByRange < PartitionRule
        def check(key_hash)
          arr = []
          key_hash.each_pair do |key, value|
            arr << "#{key} >= '#{value.begin}'"
            if value.exclude_end?
              arr << "#{key} < '#{value.end}'"
            else
              arr << "#{key} <= '#{value.end}'"
            end
          end
          arr
        end

        def name(key_hash)
          arr = []
          key_hash.each_pair do |key, value|
            case factory.determine_column_type(key)
              when :integer
                arr << "#{value.begin}"
              when :datetime,:date
                arr << value.begin.strftime("%Y%m%d")
            else
              raise PartitionError, "Unsupported key column type, #{key}"
            end
          end
          "#{arr.join('_')}"
        end
      end
    end
  end
end
