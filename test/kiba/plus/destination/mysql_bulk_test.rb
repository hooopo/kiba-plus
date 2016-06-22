require 'test_helper'

class Kiba::Plus::Destination::MysqlBulkTest < Minitest::Test

  def setup
    @options = {
      connect_url: @@connect_urls[:mysql2_dest],
      table_name: 'customers',
      columns: [:id, :email, :first_name, :last_name],
      input_file: File.join(@@test_dir, 'data/customer.csv')
    }

    @obj = Kiba::Plus::Destination::MysqlBulk.new(@options)
  end

  def test_initialize
    assert_instance_of Mysql2::Client, @obj.client
    assert_equal @options, @obj.options
  end

  def test_connect_url
    assert_equal @@connect_urls[:mysql2_dest], @obj.connect_url

    @obj.options.delete :connect_url
    assert_raises (KeyError) { @obj.connect_url }
  end

  def test_table_name
    assert_equal 'customers', @obj.table_name

    @obj.options.delete :table_name
    assert_raises (KeyError) { @obj.table_name }
  end

  def test_columns
    assert_equal [:id, :email, :first_name, :last_name], @obj.columns

    @obj.options.delete :columns
    assert_raises (KeyError) { @obj.columns }
  end

  def test_truncate
    @obj.options.delete :truncate
    assert_equal false, @obj.truncate

    @obj.options[:truncate] = true
    assert_equal true, @obj.truncate
  end

  def test_incremental
    @obj.options.delete :incremental
    assert_equal true, @obj.incremental

    @obj.options[:incremental] = false
    assert_equal false, @obj.incremental
  end

  def test_input_file
    assert_equal "#{@@test_dir}/data/customer.csv", @obj.input_file

    @obj.options.delete :input_file
    assert_raises (KeyError) { @obj.input_file }
  end

  def test_ignore_input_file_header
    @obj.options.delete :ignore_input_file_header
    assert_equal false, @obj.ignore_input_file_header

    @obj.options[:ignore_input_file_header] = true
    assert_equal true, @obj.ignore_input_file_header
  end

  def test_delimited_by
    @obj.options.delete :delimited_by
    assert_equal ',', @obj.delimited_by

    @obj.options[:delimited_by] = ' '
    assert_equal ' ', @obj.delimited_by
  end

  def test_enclosed_by
    @obj.options.delete :enclosed_by
    assert_equal '"', @obj.enclosed_by

    @obj.options[:enclosed_by] = '|'
    assert_equal '|', @obj.enclosed_by
  end

  def test_ignore_lines
    @obj.options.delete :ignore_lines
    assert_equal 0, @obj.ignore_lines

    @obj.options[:ignore_lines] = 1
    assert_equal 1, @obj.ignore_lines
  end

  def test_write
    assert_equal nil, @obj.write([])
  end

  def test_close_when_truncate
    @obj.stub :truncate, true do
      @obj.stub :truncate_sql, 'select now();' do
        @obj.stub :bulk_sql, 'select now();' do
          @obj.close
          assert_equal nil, @obj.client
        end
      end
    end
  end

  def test_close_when_non_truncate
    @obj.stub :truncate, false do
      @obj.stub :bulk_sql, 'select now();' do
        @obj.close
        assert_equal nil, @obj.client
      end
    end
  end

  def test_real_ignore_lines
    @obj.stub :ignore_lines, 1 do
      @obj.stub :ignore_input_file_header, false do
        assert_equal 1, @obj.send(:real_ignore_lines)
      end
    end

    @obj.stub :ignore_lines, 1 do
      @obj.stub :ignore_input_file_header, true do
        assert_equal 2, @obj.send(:real_ignore_lines)
      end
    end
  end

  def test_truncate_sql
    assert_equal 'TRUNCATE TABLE customers;', @obj.send(:truncate_sql)
  end

  def test_bulk_sql
    expected_sql = %Q^
    LOAD DATA LOCAL INFILE '#{@@test_dir}/data/customer.csv'
        REPLACE
        INTO TABLE customers
        FIELDS
          TERMINATED BY ','
          ENCLOSED BY '"'
        IGNORE 1 LINES
        (id,email,first_name,last_name)
    ^.gsub(/[\s]+/, ' ').strip

    @obj.stub :real_ignore_lines, 1 do
      sql = @obj.send(:bulk_sql).gsub(/[\s]+/, ' ').strip

      assert_equal expected_sql, sql
    end
  end

end
