module ActiveRecord
  module Acts #:nodoc:
    module Partitioned #:nodoc:
      class Partition < ActiveRecord::Base

        def self.set_factory(factory)
          @@factory = factory
        end

        # TODO: WHAT THE HELL??
      	def self.with_key(hash)
          self.scoped(:conditions => modified_attrs(hash)).first
        end

        # Returns true if the hash values should be stored in this partition
        def include?(hash)
          self.key.each_pair do |key, value|
            unless value === hash[key]
              return false
            end
          end
          true
        end

        def initialize(attrs = {})
          super(self.class.modified_attrs(attrs))
        end

        # TODO: Maybe we should overwrite destroy
        def drop!
          self.transaction do
            @@factory.model.connection.execute "DROP TABLE #{name}"
            self.destroy
            if num_siblings == 0
              # Delete the parent
              @@factory.model.connection.execute "DROP TABLE #{parent_name}"
            end
          end
        end

        def num_siblings
          parent = @@factory.keys[-2] # second to last
          if parent
            # TODO: This won't handle a ranged parent yet
            self.class.count(:conditions => { parent.column => attributes[parent.column] })
          end
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
          conn = @@factory.model.connection.raw_connection
          `pg_dump -h #{conn.host} -U #{conn.user} -t #{self.tablename} #{conn.db} | gzip`
        end

        def size
          rec = self.class.find(:first, :select => "relpages", :from => "pg_class", :conditions => "relname = '#{self.tablename}'")
          rec[:relpages].to_i * 8192
        end

        def name
          "#{@@factory.model.table_name}_part_#{@@factory.keys.partition_handle(:key_hash => key)}"
        end

        def parent_name
          return nil unless @@factory.keys[-2]
          "#{@@factory.model.table_name}_part_#{@@factory.keys[-2].partition_handle(:key_hash => key)}"
        end

        # Modify parameters to suit ranges if required
        private
          def self.modified_attrs(attrs)
            hash = {}
            attrs.each_pair do |key, value|
              if value.instance_of?(Range)
                hash["#{key}_begin"] = value.begin
                hash["#{key}_end"] = value.end
                hash["#{key}_exclusive"] = value.exclude_end?
              else
                hash[key] = value
              end
            end
            hash
          end

          def key
            hash = HashWithIndifferentAccess.new
            @@factory.keys.each do |k|
              case k.type
                when :continuous
                  # TODO: We do this a lot - can we DRY it up?
                  r_start = self.send("#{k.column}_begin")
                  r_end = self.send("#{k.column}_end")
                  r_exclusive = self.send("#{k.column}_exclusive")
                  hash[k.column] = Range.new(r_start, r_end, r_exclusive)
                when :discrete
                  hash[k.column] = self.send(k.column)
              end
            end
            hash
          end


      end
    end
  end
end
 
