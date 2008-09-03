# ActsAsPartitioned
module ActiveRecord
  class Base
    class << self
      def partitioned_classes
        @@subclasses[ActiveRecord::Base].select { |klass| klass.acts_as_partitioned? }
      end
    end
  end

  # TODO: An even better way to do this would be to define a class on the model eg like a has_many;
  # Weblog.partitions

  module Acts #:nodoc:
    module Partitioned #:nodoc:
      class PartitionError < StandardError ; end

      class Partition
        def foo
          puts "foo"
        end
      end
 
      def self.included(base)
        base.extend(ClassMethods)
      end

      module ClassMethods
        def acts_as_partitioned(options = {})
          sing = class << self; self; end
          sing.send(:include, ActiveRecord::Acts::Partitioned::SingletonMethods)
          sing.send(:define_method, :partition_archive?) { options[:archive] || false }
          # Zero means never dump
          sing.send(:define_method, :partition_dump_age) {
            if options[:dump_age].kind_of? Proc
              options[:dump_age].call || 0
            else
              options[:dump_age] || 0
            end
          }
          sing.send(:define_method, :acts_as_partitioned?) { true }
          class_eval "include ActiveRecord::Acts::Partitioned::InstanceMethods"
        end

        def acts_as_partitioned?
          false
        end
      end

      module InstanceMethods
        # TODO: MOve this to the Partition class instead of extending the AR Model
        def activate!
          check_partition
          schema, table = self.class.schema_and_table
          name = partition_date.strftime("%Y%m%d")
          start = partition_date.strftime("%Y-%m-%d")
          finish = (partition_date + 1).strftime("%Y-%m-%d")
          values = self.class.columns.map { |column| "NEW.#{column.name}" }
          begin
            self.class.connection.execute(<<-SQL)
              CREATE RULE #{schema}_#{table}_insert_#{name} AS
                ON INSERT TO #{self.class.table_name} WHERE
                ( date >= DATE '#{start}' AND date < DATE '#{finish}' )
                DO INSTEAD
                INSERT INTO #{self.class.table_name}_part_#{name} VALUES ( #{values.join(',')} )
            SQL
          rescue
            puts "Activation Failed: The partition is probably already active"
          end
        end

        def deactivate!
          check_partition
          schema, table = self.class.schema_and_table
          name = partition_date.strftime("%Y%m%d")
          begin
            self.class.connection.execute "DROP RULE #{schema}_#{table}_insert_#{name} ON #{self.class.table_name}"
          rescue
            puts "Deactivation Failed: The partition is probably already inactive"
          end
        end

        def drop!
          check_partition
          self.connection.execute("DROP TABLE #{self.tablename}")
        end

        def analyze!
          check_partition
          self.connection.execute("ANALYZE #{self.tablename}")
        end

        def grant_to(username)
          self.connection.execute("GRANT ALL PRIVILEGES ON #{self.tablename} TO #{username}")
        end

        def dump
          check_partition
          conn = self.class.connection.raw_connection
          `pg_dump -h #{conn.host} -U #{conn.user} -t #{self.tablename} #{conn.db} | gzip` 
        end

        def partition_date
          check_partition
          name = self.tablename.split("_").last
          Date.strptime("#{name[0..3]}-#{name[4..5]}-#{name[6..7]}", "%Y-%m-%d")
        end

        def partition_id
          check_partition
          self.tablename.split("_").last.to_i
        end

        # Bytes
        def partition_size
          check_partition
          rec = self.class.find(:first, :select => "relpages", :from => "pg_class", :conditions => "relname = '#{self.tablename}'")
          rec[:relpages].to_i * 8192
        end

        def is_partition?
          @partition == true
        end

        def set_partition!
          @partition = true
        end

        private
          def check_partition
            raise "This #{self.class} instance is not a partition" unless is_partition?
          end
      end

      # TODO: Make date configurable
      module SingletonMethods
        def create_partition(date = Date.today)
          name = date.strftime("%Y%m%d")
          start = date.strftime("%Y-%m-%d")
          finish = (date + 1).strftime("%Y-%m-%d")
          self.connection.execute(<<-SQL)
            CREATE TABLE #{self.table_name}_part_#{name} (
              CHECK ( date >= TIMESTAMP '#{start}' AND date < TIMESTAMP '#{finish}' )
            ) INHERITS (#{self.table_name});
          SQL
          # Create Indexes
          schema, table = schema_and_table
          self.get_parent_indexes.map { |str| str.gsub(table, "#{table}_part_#{name}") }.each do |indexdef|
            self.connection.execute(indexdef)
          end
          "#{self.table_name}_part_#{name}"
        end

        def find_partition(arg)
          schema, table = schema_and_table
          case arg
            when :all
              conditions = "tablename LIKE '#{table}_part_%'"
              conditions << " AND schemaname = '#{schema}'" unless schema.blank?
              parts = self.find(:all, :from => "pg_catalog.pg_tables", :conditions => conditions)
              parts.each(&:set_partition!)
              # TODO
              #parts.map do |part|
              #  Partition.new(tablename)
              #end
            when :oldest
              conditions = "tablename LIKE '#{table}_part_%'"
              conditions << " AND schemaname = '#{schema}'" unless schema.blank?
              part = self.find(:first, :from => "pg_catalog.pg_tables", :conditions => conditions, :order => "tablename")
              unless part.blank?
                part.set_partition!
              end
              part
            else
              name = arg.kind_of?(Date) ? arg.strftime("%Y%m%d") : arg
              conditions = "tablename = '#{table}_part_#{name}'"
              conditions << " AND schemaname = '#{schema}'" unless schema.blank?
              part = self.find(:first, :from => "pg_catalog.pg_tables", :conditions => conditions)
              if part.blank?
                raise "#{self.table_name} has no partition with ID = #{name}"
              end
              part.set_partition!
              part
          end
        end

        def schema_and_table
          fields = self.table_name.split('.')
          if fields.length > 1
            fields
          else
            [ "", self.table_name ]
          end
        end

        def get_parent_indexes
          schema, table = schema_and_table
          conditions = "tablename = '#{table}'"
          conditions << " and schemaname = '#{schema}'" unless schema.blank?
          self.find(:all, :from => "pg_indexes", :conditions => conditions).map(&:indexdef)
        end
      end
    end
  end
end
