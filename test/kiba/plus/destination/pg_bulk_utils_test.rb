require 'test_helper'

class Kiba::Plus::Destination::PgBulkUtilsTest < Minitest::Test

  def setup
    @options = {
      connect_url: @@connect_urls[:pg_dest],
      table_name: 'customers',
      columns: [:id, :email, :first_name, :last_name],
      input_file: File.join(@@test_dir, 'data/customer.csv')
    }

    @obj = Kiba::Plus::Destination::PgBulk.new(@options)
  end

  def test_staging_table_name
    @obj.stub :table_name, 'customers' do
      assert_equal 'customers_staging', @obj.send(:staging_table_name)
    end
  end

  def test_create_staging_table
    @obj.stub :create_staging_table_sql, 'select now()' do
      assert_instance_of PG::Result, @obj.send(:create_staging_table)
    end
  end

  def test_drop_staging_table
    @obj.stub :drop_staging_table_sql, 'select now()' do
      assert_instance_of PG::Result, @obj.send(:drop_staging_table)
    end
  end

  def test_truncate_target_table
    @obj.stub :truncate_target_table_sql, 'select now()' do
      assert_instance_of PG::Result, @obj.send(:truncate_target_table)
    end
  end

  def test_delete_before_insert
    @obj.stub :delete_before_insert_sql, 'select now()' do
      assert_instance_of PG::Result, @obj.send(:delete_before_insert)
    end
  end

  def test_merge_to_target_table
    @obj.stub :merge_to_target_table_sql, 'select now()' do
      assert_instance_of PG::Result, @obj.send(:merge_to_target_table)
    end
  end

  def test_create_staging_table_sql
    expected_sql = <<-SQL
    CREATE UNLOGGED TABLE IF NOT EXISTS customers_staging (
      LIKE customers INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES
    ) WITH (autovacuum_enabled = off)
    SQL

    @obj.stub :staging_table_name, 'customers_staging' do
      @obj.stub :table_name, 'customers' do
        sql = @obj.send(:create_staging_table_sql)

        assert_equal wrap_sql(expected_sql), wrap_sql(sql)
      end
    end
  end

  def test_drop_staging_table_sql
    expected_sql = <<-SQL
    DROP TABLE IF EXISTS customers_staging
    SQL

    @obj.stub :staging_table_name, 'customers_staging' do
      sql = @obj.send(:drop_staging_table_sql)

      assert_equal wrap_sql(expected_sql), wrap_sql(sql)
    end
  end

  def test_truncate_target_table_sql
    expected_sql = <<-SQL
    TRUNCATE TABLE customers
    SQL

    @obj.stub :table_name, 'customers' do
      sql = @obj.send(:truncate_target_table_sql)

      assert_equal wrap_sql(expected_sql), wrap_sql(sql)
    end
  end

  def test_delete_before_insert_sql
    expected_sql = <<-SQL
    DELETE FROM customers
      USING customers_staging
      WHERE customers_staging.id = customers.id
    SQL

    @obj.stub :staging_table_name, 'customers_staging' do
      @obj.stub :table_name, 'customers' do
        @obj.stub :unique_by, :id do
          sql = @obj.send(:delete_before_insert_sql)

          assert_equal wrap_sql(expected_sql), wrap_sql(sql)
        end
      end
    end
  end

  def test_merge_to_target_table_sql
    expected_sql = <<-SQL
    INSERT INTO customers
      (SELECT * FROM customers_staging)
    ON CONFLICT (id)
    DO UPDATE SET id = excluded.id, email = excluded.email, first_name = excluded.first_name, last_name = excluded.last_name
    SQL

    @obj.stub :staging_table_name, 'customers_staging' do
      @obj.stub :table_name, 'customers' do
        sql = @obj.send(:merge_to_target_table_sql)

        assert_equal wrap_sql(expected_sql), wrap_sql(sql)
      end
    end
  end

end