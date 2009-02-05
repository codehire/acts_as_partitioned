require File.join(File.dirname(__FILE__), 'test_helper')

class KeysTest < Test::Unit::TestCase
  include ActiveRecord::Acts::Partitioned

  def test_initialize
    k = Keys.new
    k << Key.new(:username)
    k << Key.new(:profile_id)
    k << Key.new(:created_at, :ranged => true)
    assert_equal k.columns, [ 'username', 'profile_id', 'created_at' ]
    assert_equal k.column_names, [ 'username', 'profile_id', 'created_at_begin', 'created_at_end' ]
    assert_equal k.remaining_columns('username'), [ 'profile_id', 'created_at_begin', 'created_at_end' ] 
    assert_equal k.remaining_columns('created_at_end'), [ 'username', 'profile_id', 'created_at_begin' ] 
  end
end
