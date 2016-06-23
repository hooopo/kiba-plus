require 'test_helper'

class Kiba::Features::MysqlToXTest < Minitest::Test

  attr_reader :src_db, :src_url
  attr_reader :dest_pg_db, :dest_pg_url, :dest_csv_path

  def build
    @src_db = @@sequel_dbs[:mysql2_src]
    @src_url = @@connect_urls[:mysql2_src]

    @dest_pg_db = @@sequel_dbs[:pg_dest]
    @dest_pg_url = @@connect_urls[:pg_dest]

    @dest_csv_path = make_csv_file

    src_db.create_table! :customers do
      primary_key :id
      column :email, String
    end
    1.upto(10).each do |n|
      src_db[:customers].insert id: n, email: "user#{n}@example.com"
    end

    if dest_pg_db.table_exists? :customers_staging
      dest_pg_db.drop_table :customers_staging
    end
    if dest_pg_db.table_exists? :customers
      dest_pg_db.drop_table :customers
    end
    dest_pg_db.create_table! :customers do
      primary_key :id
      column :email, String
      column :first_name, String
      column :last_name, String
    end
  end

  def test_to_pg_with_examples_customer_mysql_to_pg
    build

    etl_content = %Q^
require 'kiba/plus'

SOURCE_URL = '#{src_url}'
DEST_URL   = '#{dest_pg_url}'

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
    run_etl_content etl_content

    assert_equal 10, dest_pg_db[:customers].count
    assert_equal 'user10@example.com', dest_pg_db[:customers].order(:id).last[:email]
  end

  def test_to_csv_with_examples_customer_mysql_to_csv
    build

    etl_content = %Q^
require 'kiba/plus'

SOURCE_URL = '#{src_url}'

source Kiba::Plus::Source::Mysql, :connect_url => SOURCE_URL,
                           :query => %Q{SELECT id, email, 'hooopo' AS first_name, 'Wang' AS last_name FROM customers}

destination Kiba::Plus::Destination::Csv, :output_file => '#{dest_csv_path}'


post_process do
end
^
    run_etl_content etl_content

    csv_rows = CSV.read(dest_csv_path)

    assert_equal 10, csv_rows.size
    assert_equal 'user10@example.com', csv_rows.last[1]
  end

end
