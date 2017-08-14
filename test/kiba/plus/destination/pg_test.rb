require 'test_helper'

require "kiba/plus/destination/pg"

class Kiba::Plus::Destination::PgTest < Minitest::Test


  def before_setup
    super

    dest_pg_db = @@sequel_dbs[:pg_dest]

    if dest_pg_db.table_exists? :customers_staging
      dest_pg_db.drop_table :customers_staging
    end
    dest_pg_db.create_table! :customers do
      primary_key :id
      column :email, String
      column :first_name, String
      column :last_name, String
    end

    @dest_pg_db = dest_pg_db
  end

  def setup
    @options = {
      connect_url: @@connect_urls[:pg_dest],
      columns: [:id, :email, :first_name, :last_name],
      table_name: 'customers'
    }

    @obj = Kiba::Plus::Destination::Pg.new(@options)
  end

  def test_initialize
    assert_instance_of PG::Connection, @obj.conn
    assert_equal @options, @obj.options
  end

  def test_write_when_row_id_is_null
    exception = assert_raises (PG::Error) { @obj.write({id: nil}) }

    assert_match /null value in column "id"/, exception.message
  end

  def test_write_when_default
    row = {id: 1, email: "user1@example.com", first_name: 'foo', last_name: 'bar'}
    result = @obj.write(row)

    assert_instance_of PG::Result, result

    assert_equal 1, @dest_pg_db[:customers].count
    assert_equal 'user1@example.com', @dest_pg_db[:customers].order(:id).last[:email]
  end

  def test_write_when_twice
    row = {id: 1, email: "user1@example.com", first_name: 'foo', last_name: 'bar'}
    @obj.write(row)

    exception = assert_raises (PG::Error) { @obj.write(row) }
    assert_match %r{duplicate key value violates unique constraint "customers_pkey"}, exception.message
  end

  def test_close
    @obj.close
    assert_nil @obj.conn
  end

  def test_connect_url
    assert_equal @@connect_urls[:pg_dest], @obj.send(:connect_url)

    @obj.options.delete :connect_url
    assert_raises (KeyError) { @obj.send(:connect_url) }
  end


  def test_columns
    assert_equal [:id, :email, :first_name, :last_name], @obj.send(:columns)

    @obj.options.delete :columns
    assert_raises (KeyError) { @obj.send(:columns) }
  end


end
