
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

      class PartitionRule
        attr_reader :factory

        def initialize(factory)
          @factory = factory
        end

        def table_name
          factory.model.table_name
        end
      end

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

      class Factory
        attr_reader :model

        # Key can be a symbol or an array of symbols
        # Dates are assumed to be ranges by day
        def initialize(model, key, options = {})
          @model = model
          spl = @model.table_name.split(".")
          @schema_name, @table_name = if spl.size > 1
            [ nil, spl[0] ]
          else
            spl
          end
          @key = key
          # TODO: Raise if model does not have key column(s)
          @options = options
        end

        # Arguments are the keys specified in creation as a hash
        # eg: create(:date => Date.today, :domain => domain)
        # TODO: Erb?
        def create(key_hash)
          # TODO: Raise if a key missing
          #self.connection.execute(<<-SQL)
          puts(<<-SQL)
            CREATE TABLE #{table_name}_part_#{partition_rule.name(key_hash)} (
              CHECK (#{partition_rule.check(key_hash).join(' AND ')})
            ) INHERITS (#{table_name});
          SQL
          # TODO: Indexes
          # TODO: Return the partition object
        end

        def find(arg)
          if arg == :all
            puts "All"
            conditions = "tablename LIKE '#{@table_name}_part_%'"
            conditions << if @schema_name
              " AND schemaname = '#{@schema_name}'"
            else
              " AND schemaname = current_schema()"
            end
            parts = self.find(:all, :from => "pg_catalog.pg_tables", :conditions => conditions)
            # TODO: Convert to partition objects
          else
            puts partition_rule.name(arg)
          end
        end

        def determine_column_type(column)
          @model.columns.detect do |c|
            c.name == column.to_s
          end.type
        end

        private
          def partition_rule
            if @options.has_key?(:rule)
              rule = @options[:rule]
              case rule
                when Symbol
                  rule.to_s.constantize.new(self)
                when Class
                  rule.new(self)
              end
            else
              ByValue.new(self)
            end
          end

          def table_name
            if @schema_name
              "#{@schema_name}.#{@table_name}"
            else
              @table_name
            end
          end
      end

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
 
      def self.included(base)
        e.extend(ClassMethods)
      end

      module ClassMethods
        def acts_as_partitioned(options = {})
          sing = class << self; self; end
          
          sing.send(:define_method, :partitions) {
            Factory.new(self)
          }

=begin
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
=end
        end

        def acts_as_partitioned?
          false
        end
      end
    end
  end
end
