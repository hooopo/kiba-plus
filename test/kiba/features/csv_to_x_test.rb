require 'test_helper'

class Kiba::Features::CsvToXTest < Minitest::Test

  attr_reader :src_csv_path
  attr_reader :dest_mysql2_db, :dest_mysql2_url, :dest_pg_db, :dest_pg_url

  def build
    @src_csv_path = make_csv_file

    @dest_mysql2_db = @@sequel_dbs[:mysql2_dest]
    @dest_mysql2_url = @@connect_urls[:mysql2_dest]

    @dest_pg_db = @@sequel_dbs[:pg_dest]
    @dest_pg_url = @@connect_urls[:pg_dest]

    # default mode is 0600
    FileUtils.chmod 0666, src_csv_path
    CSV.open(src_csv_path, "wb") do |csv|
      1.upto(10).each do |n|
        csv << [n, "user#{n}@example.com", "first_name#{n}", "last_name#{n}"]
      end
    end

    dest_mysql2_db.create_table! :customers do
      primary_key :id
      column :email, String
      column :first_name, String
      column :last_name, String
    end


    if dest_pg_db.table_exists? :customers_staging
      dest_pg_db.drop_table :customers_staging
    end
    dest_pg_db.create_table! :customers do
      primary_key :id
      column :email, String
      column :first_name, String
      column :last_name, String
    end
  end

  def test_to_mysql_with_examples_customer_csv_to_mysql
    build

    etl_content = %Q^
require 'kiba/plus'

DEST_URL   = '#{dest_mysql2_url}'

destination Kiba::Plus::Destination::MysqlBulk, { :connect_url => DEST_URL,
                                :table_name => "customers",
                                :input_file => '#{src_csv_path}',
                                :truncate => true,
                                :columns => [:id, :email, :first_name, :last_name],
                                :incremental => false
                              }

post_process do
end
^
    run_etl_content etl_content

    assert_equal 10, dest_mysql2_db[:customers].count
    assert_equal 'user10@example.com', dest_mysql2_db[:customers].order(:id).last[:email]
  end

  def test_to_pg_with_examples_customer_csv_to_pg
    build

    #
    # Because csv file should not in /tmp dir
    #
    pg_copy_tmp_dir = File.expand_path('../../../pg_copy_tmp', __FILE__)
    src_csv_path_with_pg = File.join pg_copy_tmp_dir, File.basename(src_csv_path)
    FileUtils.cp src_csv_path, src_csv_path_with_pg

    etl_content = %Q^
require 'kiba/plus'

DEST_URL   = '#{dest_pg_url}'

destination Kiba::Plus::Destination::PgBulk, { :connect_url => DEST_URL,
                                :table_name => "customers",
                                :input_file => '#{src_csv_path_with_pg}',
                                :truncate => true,
                                :columns => [:id, :email, :first_name, :last_name],
                                :incremental => false
                              }

post_process do
end
^
    run_etl_content etl_content

    FileUtils.rm_rf src_csv_path_with_pg

    assert_equal 10, dest_pg_db[:customers].count
    assert_equal 'user10@example.com', dest_pg_db[:customers].order(:id).last[:email]
  end

end
