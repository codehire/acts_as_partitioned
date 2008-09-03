# Include hook code here
require 'acts_as_partitioned'
ActiveRecord::Base.send(:include, ActiveRecord::Acts::Partitioned)
