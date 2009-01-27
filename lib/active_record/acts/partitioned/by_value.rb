
module ActiveRecord
  module Acts #:nodoc:
    module Partitioned #:nodoc:
      class ByValue < PartitionRule
        def check(key_hash)
          arr = []
          key_hash.each_pair do |key, value|
            arr << "#{key} = '#{value}'"
          end
          arr
        end

        def name(key_hash)
          arr = []
          key_hash.each_pair do |key, value|
            case factory.determine_column_type(key)
              when :integer
                arr << "#{value}"
              when :datetime,:date
                arr << value.strftime("%Y%m%d")
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

