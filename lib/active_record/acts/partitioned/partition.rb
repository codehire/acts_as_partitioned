module ActiveRecord
  module Acts #:nodoc:
    module Partitioned #:nodoc:
      # TODO: Maybe we use a catalog system like to active report
      class Partition
        # TODO: How do we return the constraints with which the partition was created?? - from the name?
        def initialize(factory, name, key)
          @model = model
          @name = name
          @key = key
        end

        def activate!

        end

        def deactivate!

        end

        def drop!

        end

        def dump

        end

        def size

        end
      end
    end
  end
end
 
