require File.join(File.dirname(__FILE__), 'test_helper')

class PartitionCacheTest < Test::Unit::TestCase
  include ActiveRecord::Acts::Partitioned

  def setup
    @keys = Keys.new
    @keys << Key.new(:profile_id)
    @keys << Key.new(:created_at, :ranged => true)
  end

  def test_add_and_find
    cache = Cache::PartitionCache.new(@keys)
    cache.add(Item.partitions.find(:first))
    assert_equal cache.size, 1 
    # Find exact (==)
    found = cache.find(Item.partitions.find(:first).key)
    assert found
    assert_equal found, Item.partitions.find(:first)
    # Find in range (===)
    found = cache.find(:profile_id => 1, :created_at => (Time.today - 1.day - 2.hours))
    assert found
    assert_equal found, Item.partitions.find(:first)
    # Find with superfluous options
    found = cache.find(:profile_id => 1, :created_at => (Time.today - 1.day - 2.hours), :foo => 'bar')
    assert found
    assert_equal found, Item.partitions.find(:first)
  end

  def test_duplicates
    cache = Cache::PartitionCache.new(@keys)
    cache.add(Item.partitions.find(:first))
    assert_equal cache.size, 1 
    cache.add(Item.partitions.find(:first))
    # Should still be 1
    assert_equal cache.size, 1 
    cache.add(Item.partitions.find(:last))
    assert_equal cache.size, 2
  end
end
