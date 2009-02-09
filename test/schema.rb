ActiveRecord::Schema.define :version => 0 do

  execute "DROP TABLE items CASCADE"

  create_table :items, :force => true do |t|
    t.column :created_at, :timestamp
    t.column :username, :string
    t.references :profile
  end

end
