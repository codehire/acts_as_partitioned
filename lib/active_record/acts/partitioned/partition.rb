module ActiveRecord
  module Acts #:nodoc:
    module Partitioned #:nodoc:
      class Partition < ActiveRecord::Base

        def self.set_factory(factory)
          @@factory = factory
        end

        def self.set_keys(keys)
          @@keys = keys
        end

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

        # TODO: Do we really need this?
        #def activate!
        #end
        #def deactivate!
        #end

        def key
          hash = HashWithIndifferentAccess.new
          @@keys.each do |k|
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

        def drop!
	  self.transaction do
	    @@factory.model.connection.execute <<-SQL
	      DROP TABLE #{name}
	    SQL
	    self.destroy
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
          "#{@@factory.model.table_name}_part_#{id}"
        end

        def copy_into(hash)
          @copy_file ||= CopyFile.new(self.name)
          @copy_file << hash
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
      end
    end
  end
end
 
