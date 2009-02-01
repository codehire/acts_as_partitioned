require 'active_record/acts/partitioned/factory'
require 'active_record/acts/partitioned/structure'
require 'active_record/acts/partitioned/copy_proxy'

# ActsAsPartitioned
module ActiveRecord
  class Base
    class << self
      def partitioned_classes
        @@subclasses[ActiveRecord::Base].select(&:partitioned?)
      end
    end
  end

  module Acts #:nodoc:
    module Partitioned #:nodoc:
      class PartitionError < StandardError ; end
 
      def self.included(base)
        base.extend(ClassMethods)
      end

      module ClassMethods
	def partition(*args)
          if args.last.instance_of?(Hash)
            options = args.last
            key = args[0...-1]
          else
            options = {}
            key = args
          end
          sing = class << self; self; end
          eval <<-EVAL
            class ActiveRecord::Acts::Partitioned::#{self.name}Partition < ActiveRecord::Acts::Partitioned::Partition
              set_table_name '#{self.table_name.singularize}_partitions'
            end
          EVAL
          klass = "ActiveRecord::Acts::Partitioned::#{self.name}Partition".constantize
	  factory = Factory.new(self, klass, options)
	  args.each { |arg| factory.partition_by(key) }
	  yield factory if block_given?
          factory.set_validations
          sing.send(:define_method, :partitions) { factory }
          sing.send(:define_method, :partitioned?) { true }
          # TODO: Put this in sep rake task and call on factory - should this be called Proxy
	  #factory.migrate(:force => true)
	end
      end
    end
  end
end
