require File.join(File.dirname(__FILE__), 'test_helper')

class FactoryTest < Test::Unit::TestCase
  include ActiveRecord::Acts::Partitioned

  def test_create
    assert Item.partitions
    Item.partitions.clear
    assert_equal Item.partitions.count, 0
    # Create a partition
    Item.partitions.create(:created_at => (Date.today.to_time - 2.days)...(Date.today.to_time - 1.day), :profile_id => 2)
    assert_equal Item.partitions.count, 1
    Item.connection.tables.include?("weblogs_part_2")
    Item.connection.tables.include?("weblogs_part_2_#{(Date.today.to_time - 2.days).strftime("%Y%m%d%H%M")}_#{(Date.today.to_time - 1.day).strftime("%Y%m%d%H%M")}")
    # Create another
    Item.partitions.create(:created_at => (Date.today.to_time - 2.days)...(Date.today.to_time - 1.day), :profile_id => 1)
    assert_equal Item.partitions.count, 2
    Item.connection.tables.include?("weblogs_part_1")
    Item.connection.tables.include?("weblogs_part_1_#{(Date.today.to_time - 2.days).strftime("%Y%m%d%H%M")}_#{(Date.today.to_time - 1.day).strftime("%Y%m%d%H%M")}")
    # Clear the partitions
    Item.partitions.clear
    assert_equal Item.partitions.count, 0
  end

  def test_overlap_simple
    assert User.partitions
    User.partitions.clear
    assert_equal User.partitions.count, 0
    User.partitions.create(:group_id => 100)
    assert_equal User.partitions.count, 1
    Item.connection.tables.include?("users_part_100")
    assert_raise(ActiveRecord::RecordInvalid) {
      User.partitions.create(:group_id => 100)
    }
    assert_equal User.partitions.count, 1
  end

  def test_overlap_ranged
    assert Item.partitions
    Item.partitions.clear
    assert_equal Item.partitions.count, 0
    Item.partitions.create(:created_at => (Date.today.to_time - 3.days)...(Date.today.to_time - 1.day), :profile_id => 2)
    assert_equal Item.partitions.count, 1
    assert_raise(ActiveRecord::RecordInvalid) {
      Item.partitions.create(:created_at => (Date.today.to_time - 2.days)...(Date.today.to_time - 1.day), :profile_id => 2)
    }
    assert_equal User.partitions.count, 1
  end
end
