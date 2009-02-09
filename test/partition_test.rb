require File.join(File.dirname(__FILE__), 'test_helper')

class PartitionTest < Test::Unit::TestCase
  include ActiveRecord::Acts::Partitioned

  def test_add_and_find
  end
end
