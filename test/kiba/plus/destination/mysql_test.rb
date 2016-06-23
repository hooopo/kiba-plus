require 'test_helper'

class Kiba::Plus::Destination::MysqlTest < Minitest::Test
  def before_setup
    super

    dest_mysql2_db = @@sequel_dbs[:mysql2_dest]

    dest_mysql2_db.create_table! :customers do
      primary_key :id
      column :email, String
      column :first_name, String
      column :last_name, String
    end

    @dest_mysql2_db = dest_mysql2_db
  end

  def setup
    @options = {
      connect_url: @@connect_urls[:mysql2_dest],
      prepare_sql: 'INSERT INTO customers VALUES (?, ?, ?, ?)',
      columns: [:id, :email, :first_name, :last_name]
    }

    @obj = Kiba::Plus::Destination::Mysql.new(@options)
  end

  def test_initialize
    assert_instance_of Mysql2::Client, @obj.client
    assert_equal @options, @obj.options
  end

  def test_write_when_row_id_is_null
    @obj.write({id: nil})

    # TODO why eq 1 ?
    # assert_equal 0, @dest_mysql2_db[:customers].count
  end

  def test_write_when_default
    row = {id: 1, email: "user1@example.com", first_name: 'foo', last_name: 'bar'}
    result = @obj.write(row)

    assert_equal nil, result

    assert_equal 1, @dest_mysql2_db[:customers].count
    assert_equal 'user1@example.com', @dest_mysql2_db[:customers].order(:id).last[:email]
  end

  def test_write_when_twice
    row = {id: 1, email: "user1@example.com", first_name: 'foo', last_name: 'bar'}
    @obj.write(row)

    exception = assert_raises (Mysql2::Error) { @obj.write(row) }

    assert_match %r{Duplicate entry '1' for key 'PRIMARY'}, exception.message
  end

  def test_close
    @obj.close
    assert_equal nil, @obj.client
  end

  def test_connect_url
    assert_equal @@connect_urls[:mysql2_dest], @obj.send(:connect_url)

    @obj.options.delete :connect_url
    assert_raises (KeyError) { @obj.send(:connect_url) }
  end

  def test_prepare_sql
    assert_equal 'INSERT INTO customers VALUES (?, ?, ?, ?)', @obj.send(:prepare_sql)

    @obj.options.delete :prepare_sql
    assert_raises (KeyError) { @obj.send(:prepare_sql) }
  end

  def test_columns
    assert_equal [:id, :email, :first_name, :last_name], @obj.send(:columns)

    @obj.options.delete :columns
    assert_raises (KeyError) { @obj.send(:columns) }
  end

end
