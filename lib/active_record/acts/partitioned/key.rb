module ActiveRecord
  module Acts #:nodoc:
    module Partitioned #:nodoc:
      class Key
        attr_accessor :column, :type, :using

	def initialize(column, options = {})
	  @column = column.to_s
	  @type = options[:ranged] ? :continuous : :discrete
	end
      end
    end
  end
end
