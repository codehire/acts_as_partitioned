ActiveRecord::Schema.define :version => 0 do

  begin; execute "DROP TABLE items CASCADE"; rescue; end
  begin; execute "DROP TABLE data_logs CASCADE"; rescue; end
  begin; execute "DROP TABLE users CASCADE"; rescue; end

  create_table :items, :force => true do |t|
    t.column :created_at, :timestamp
    t.column :username, :string
    t.references :profile
  end

  create_table :data_logs, :force => true do |t|
    t.column :created_at, :timestamp
    t.column :data, :string
    t.references :account
  end

  create_table :users, :force => true do |t|
    t.column :name, :string
    t.references :group
  end

end
