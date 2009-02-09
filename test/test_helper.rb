$:.unshift(File.dirname(__FILE__) + '/../lib')

require 'test/unit'
require File.expand_path(File.join(File.dirname(__FILE__), '../../../../config/environment.rb'))
require 'rubygems'
#require 'active_support/breakpoint'
require 'active_record/fixtures'

config = YAML::load(IO.read(File.dirname(__FILE__) + '/database.yml'))
ActiveRecord::Base.configurations = YAML.load_file(File.dirname(__FILE__) + '/database.yml')
ActiveRecord::Base.logger = Logger.new(File.dirname(__FILE__) + "/debug.log")
ActiveRecord::Base.establish_connection(ENV['DB'] || 'test')

load(File.dirname(__FILE__) + "/schema.rb")

class Item < ActiveRecord::Base
  partition do |part|
    part.partition_by :created_at, :ranged => true
    part.partition_by :profile_id
  end
end

Item.partitions.migrate(:force => true)
Item.partitions.create(:created_at => (Time.today - 2.days)...(Time.today - 1.day), :profile_id => 1)
Item.partitions.create(:created_at => (Time.today - 2.days)...(Time.today - 1.day), :profile_id => 2)
