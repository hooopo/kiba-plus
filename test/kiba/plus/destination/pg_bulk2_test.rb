# require 'test_helper'

# class Kiba::Plus::Destination::PgBulk2Test < Minitest::Test


#   def before_setup
#     super

#     dest_pg_db = @@sequel_dbs[:pg_dest]

#     if dest_pg_db.table_exists? :customers_staging
#       dest_pg_db.drop_table :customers_staging
#     end
#     if dest_pg_db.table_exists? :customers
#       dest_pg_db.drop_table :customers
#     end
#     dest_pg_db.create_table! :customers do
#       primary_key :id
#       column :email, String
#       column :first_name, String
#       column :last_name, String
#     end
#   end

#   def setup
#     @options = {
#       connect_url: @@connect_urls[:pg_dest],
#       table_name: 'customers',
#       columns: [:id, :email, :first_name, :last_name]
#     }

#     @obj = Kiba::Plus::Destination::PgBulk2.new(@options)
#   end

#   def test_initialize
#     assert_instance_of PG::Connection, @obj.conn
#     assert_equal @options, @obj.options
#   end

#   def test_connect_url
#     assert_equal @@connect_urls[:pg_dest], @obj.connect_url

#     @obj.options.delete :connect_url
#     assert_raises (KeyError) { @obj.connect_url }
#   end

#   def test_table_name
#     assert_equal 'customers', @obj.table_name

#     @obj.options.delete :table_name
#     assert_raises (KeyError) { @obj.table_name }
#   end

#   def test_columns
#     assert_equal [:id, :email, :first_name, :last_name], @obj.columns

#     @obj.options.delete :columns
#     assert_raises (KeyError) { @obj.columns }
#   end

#   def test_truncate
#     @obj.options.delete :truncate
#     assert_equal false, @obj.truncate

#     @obj.options[:truncate] = true
#     assert_equal true, @obj.truncate
#   end

#   def test_incremental
#     @obj.options.delete :incremental
#     assert_equal true, @obj.incremental

#     @obj.options[:incremental] = false
#     assert_equal false, @obj.incremental
#   end

#   def test_unique_by
#     @obj.options.delete :unique_by
#     assert_equal :id, @obj.unique_by

#     @obj.options[:unique_by] = :uuid
#     assert_equal :uuid, @obj.unique_by
#   end

#   def test_write
#     # TODO
#   end

#   def test_close
#     # TODO
#   end

#   def test_bulk_sql_with_incremental
#     expected_sql = %Q^
#     COPY customers_staging (id, email, first_name, last_name)
#         FROM STDIN
#           WITH
#             DELIMITER ','
#             NULL '\\N'
#             CSV
#     ^.gsub(/[\s]+/, ' ').strip

#     @obj.stub :staging_table_name, 'customers_staging' do
#       sql = @obj.send(:bulk_sql_with_incremental).gsub(/[\s]+/, ' ').strip

#       assert_equal expected_sql, sql
#     end
#   end

#   def test_bulk_sql_with_non_incremental
#     expected_sql = %Q^
#     COPY customers (id, email, first_name, last_name)
#         FROM STDIN
#           WITH
#             DELIMITER ','
#             NULL '\\N'
#             CSV
#     ^.gsub(/[\s]+/, ' ').strip

#     @obj.stub :table_name, 'customers' do
#       sql = @obj.send(:bulk_sql_with_non_incremental).gsub(/[\s]+/, ' ').strip

#       assert_equal expected_sql, sql
#     end
#   end

# end
