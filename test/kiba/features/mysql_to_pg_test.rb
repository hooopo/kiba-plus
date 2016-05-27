require 'test_helper'

class Kiba::Features::MysqlToPgTest < Minitest::Test

  def test_with_examples_customer_mysql_to_pg
    url_src = @@connect_urls[:mysql2_src]
    url_dest = @@connect_urls[:pg_dest]

    db_src = @@sequel_dbs[:mysql2_src]
    db_dest = @@sequel_dbs[:pg_dest]

    db_src.create_table! :customers do
      primary_key :id
      column :email, String
    end
    1.upto(10).each do |n|
      db_src[:customers].insert id: n, email: "user#{n}@example.com"
    end

    db_dest.create_table! :customers do
      primary_key :id
      column :email, String
      column :first_name, String
      column :last_name, String
    end

    etl = %Q^
require 'kiba/plus'

SOURCE_URL = '#{url_src}'
DEST_URL   = '#{url_dest}'

source Kiba::Plus::Source::Mysql, { :connect_url => SOURCE_URL,
                           :query => %Q{SELECT id, email, 'hooopo' AS first_name, 'Wang' AS last_name FROM customers}
                         }

destination Kiba::Plus::Destination::PgBulk2, { :connect_url => DEST_URL,
                                :table_name => "customers",
                                :truncate => true,
                                :columns => [:id, :email, :first_name, :last_name],
                                :incremental => false
                              }

post_process do
end
^
    run_etl etl

    assert_equal 10, db_dest[:customers].count
    assert_equal 'user10@example.com', db_dest[:customers].order(:id).last[:email]
  end

end
