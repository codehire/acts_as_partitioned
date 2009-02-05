module ActiveRecord
  module Acts #:nodoc:
    module Partitioned #:nodoc:
      class Keys < Array
	def columns
	  self.map(&:column)
	end

        def column_names
          self.inject([]) do |names, key|
            case key.type
              when :continuous
                names << "#{key.column}_begin"
                names << "#{key.column}_end"
              when :discrete
                names << key.column
            end
          end
        end

        # Returns the list of column names excluding this one
        def remaining_columns(column)
          self.column_names - [column]
        end
      end
    end
  end
end
