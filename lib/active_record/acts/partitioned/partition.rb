module ActiveRecord
  module Acts #:nodoc:
    module Partitioned #:nodoc:
      # TODO: Maybe we use a catalog system like to active report
      class Partition < ActiveRecord::Base

        def self.set_factory(factory)
          @@factory = factory
        end

        # TODO: Add validation
        def initialize(attrs = {})
          super(modified_attrs(attrs))
        end

        # TODO: Do we really need this?
        #def activate!
        #end
        #def deactivate!
        #end

        def drop!

        end

        # Will unlink the partition from the parent table but not delete
        def unlink
          self.transaction do
            @@factory.model.connection.execute <<-SQL
              ALTER TABLE #{name} NO INHERIT #{@@factory.model.table_name};
              ALTER TABLE #{name} RENAME TO #{name}_unlinked;
            SQL
            self.destroy
          end
        end

        def dump

        end

        def size

        end

        def name
          "#{@@factory.model.table_name}_part_#{id}"
        end

        # Modify parameters to suit ranges if required
        private
          def modified_attrs(attrs)
            hash = {}
            attrs.each_pair do |key, value|
              if value.instance_of? Range
                hash["#{key}_begin"] = value.begin
                hash["#{key}_end"] = value.end
                hash["#{key}_exclusive"] = value.exclude_end?
              else
                hash[key] = value
              end
            end
            puts "Modified hash = #{hash.inspect}"
            hash
          end
      end
    end
  end
end
 
