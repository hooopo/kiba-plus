require 'test_helper'

class Kiba::Plus::Destination::PgBulk2Test < Minitest::Test


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
  end

  def setup
    @options = {
      connect_url: @@connect_urls[:pg_dest],
      table_name: 'customers',
      columns: [:id, :email, :first_name, :last_name]
    }

    @obj = Kiba::Plus::Destination::PgBulk2.new(@options)
  end

  def teardown
    super

    if @obj.conn
      @obj.conn.put_copy_end
    end
  end

  def test_initialize
    assert_instance_of PG::Connection, @obj.conn
    assert_equal @options, @obj.options
  end


  def test_write
    # TODO
  end

  def test_close
    # TODO
  end

  def test_connect_url
    assert_equal @@connect_urls[:pg_dest], @obj.send(:connect_url)

    @obj.options.delete :connect_url
    assert_raises (KeyError) { @obj.send(:connect_url) }
  end

  def test_table_name
    assert_equal 'customers', @obj.send(:table_name)

    @obj.options.delete :table_name
    assert_raises (KeyError) { @obj.send(:table_name) }
  end

  def test_columns
    assert_equal [:id, :email, :first_name, :last_name], @obj.send(:columns)

    @obj.options.delete :columns
    assert_raises (KeyError) { @obj.send(:columns) }
  end

  def test_truncate
    @obj.options.delete :truncate
    assert_equal false, @obj.send(:truncate)

    @obj.options[:truncate] = true
    assert_equal true, @obj.send(:truncate)
  end

  def test_incremental
    @obj.options.delete :incremental
    assert_equal true, @obj.send(:incremental)

    @obj.options[:incremental] = false
    assert_equal false, @obj.send(:incremental)
  end

  def test_unique_by
    @obj.options.delete :unique_by
    assert_equal :id, @obj.send(:unique_by)

    @obj.options[:unique_by] = :uuid
    assert_equal :uuid, @obj.send(:unique_by)
  end


  def test_bulk_sql_with_incremental
    expected_sql = <<-SQL
    COPY customers_staging (id, email, first_name, last_name)
        FROM STDIN
          WITH
            DELIMITER ','
            NULL '\\N'
            CSV
    SQL

    @obj.stub :staging_table_name, 'customers_staging' do
      sql = @obj.send(:bulk_sql_with_incremental)

      assert_equal wrap_sql(expected_sql), wrap_sql(sql)
    end
  end

  def test_bulk_sql_with_non_incremental
    expected_sql = <<-SQL
    COPY customers (id, email, first_name, last_name)
        FROM STDIN
          WITH
            DELIMITER ','
            NULL '\\N'
            CSV
    SQL

    @obj.stub :table_name, 'customers' do
      sql = @obj.send(:bulk_sql_with_non_incremental)

      assert_equal wrap_sql(expected_sql), wrap_sql(sql)
    end
  end

end
