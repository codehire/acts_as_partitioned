require 'active_record/acts/partitioned/partition_rule'
require 'active_record/acts/partitioned/by_value'
require 'active_record/acts/partitioned/by_range'
require 'active_record/acts/partitioned/factory'
require 'active_record/acts/partitioned/structure'

puts "INCLUDING?"

# ActsAsPartitioned
module ActiveRecord
  class Base
    class << self
      def partitioned_classes
        @@subclasses[ActiveRecord::Base].select { |klass| klass.acts_as_partitioned? }
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
        def acts_as_partitioned(*args)
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
            end
          EVAL
          # TODO: Put this in sep rake task
          Structure.init_partition_catalog(self, key, :force => true)
          klass = "ActiveRecord::Acts::Partitioned::#{self.name}Partition".constantize
          sing.send(:define_method, :partitions) {
            Factory.new(self, key, klass, options)
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
      end
    end
  end
end
