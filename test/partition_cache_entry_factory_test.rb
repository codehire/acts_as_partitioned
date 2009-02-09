require File.join(File.dirname(__FILE__), 'test_helper')

class PartitionCacheEntryFactoryTest < Test::Unit::TestCase
  include ActiveRecord::Acts::Partitioned

  def setup
    @keys = Keys.new
    @keys << Key.new(:username)
    @keys << Key.new(:profile_id)
    @keys << Key.new(:created_at, :ranged => true)
  end

  def test_match
    @fact = Cache::PartitionCacheEntryFactory.new(@keys)
    entry = @fact.create
    assert entry
    assert_instance_of Cache::PartitionCacheEntry, entry
    assert entry.methods.include?("username=")
    assert entry.methods.include?("profile_id=")
    assert entry.methods.include?("created_at=")
    entry.username = "daniel"
    assert_equal entry.instance_variable_get("@username"), "daniel"
  end
end
