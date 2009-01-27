# Include hook code here
require 'active_record/acts/partitioned'
ActiveRecord::Base.send(:include, ActiveRecord::Acts::Partitioned)
