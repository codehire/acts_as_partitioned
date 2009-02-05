require File.join(File.dirname(__FILE__), 'test_helper')

class KeyTest < Test::Unit::TestCase
  include ActiveRecord::Acts::Partitioned

  def test_initialize
    k = Key.new(:username)
    assert_equal(k.type, :discrete)
    assert_equal(k.column, 'username')
    k = Key.new(:date, :ranged => true)
    assert_equal(k.type, :continuous)
  end
end
