
module ActiveRecord
  module Acts #:nodoc:
    module Partitioned #:nodoc:
      class PartitionRule
        attr_reader :factory

        def initialize(factory)
          @factory = factory
        end

        def table_name
          factory.model.table_name
        end
      end
    end
  end
end

