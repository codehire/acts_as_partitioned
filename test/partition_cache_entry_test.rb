require File.join(File.dirname(__FILE__), 'test_helper')

class MyTestEntry < ActiveRecord::Acts::Partitioned::Cache::PartitionCacheEntry
  attr_writer :username, :profile_id, :created_at
end

class PartitionCacheEntryTest < Test::Unit::TestCase
  include ActiveRecord::Acts::Partitioned

  def setup
    @keys = Keys.new
    @keys << Key.new(:username)
    @keys << Key.new(:profile_id)
    @keys << Key.new(:created_at, :ranged => true)
  end

  def test_match
    @entry = MyTestEntry.new(@keys)
    @entry.username = "daniel"
    @entry.profile_id = 1
    @entry.created_at = (Date.today.to_time - 5.days)...(Date.today.to_time + 1.day)
    # Test within range (===)
    assert @entry == { :username => 'daniel', :profile_id => 1, :created_at => Time.now }
    # Test exact match (==)
    assert @entry == { :username => 'daniel', :profile_id => 1, :created_at => (Date.today.to_time - 5.days)...(Date.today.to_time + 1.day) }
    assert !(@entry == { :username => 'da', :profile_id => 2, :created_at => Time.now })
    assert_raise(RuntimeError) {
      @entry == { :profile_id => 1, :created_at => Time.now }
    }
  end
end
