require 'test_helper'

class Kiba::Plus::Source::MysqlTest < Minitest::Test

  def setup
    @options = {
      connect_url: @@connect_urls[:mysql2_src],
      query: 'select id, email from customers'
    }

    @obj = Kiba::Plus::Source::Mysql.new(@options)
  end

  def test_initialize_with_simple_options
    assert_instance_of Mysql2::Client, @obj.client
    assert_equal @options, @obj.options
  end

  def test_each
    src_db = @@sequel_dbs[:mysql2_src]
    src_db.create_table! :customers do
      primary_key :id
      column :email, String
    end
    1.upto(10).each do |n|
      src_db[:customers].insert id: n, email: "user#{n}@example.com"
    end

    rows = []
    @obj.each{|row| rows << row}

    assert_equal 10, rows.count
    assert_equal 'user10@example.com', rows.last[:email]
  end

  def test_connect_url
    assert_equal @@connect_urls[:mysql2_src], @obj.send(:connect_url)

    @obj.options.delete :connect_url
    assert_raises (KeyError) { @obj.send(:connect_url) }
  end

  def test_query
    assert_equal 'select id, email from customers', @obj.send(:query)

    @obj.options.delete :query
    assert_raises (KeyError) { @obj.send(:query) }
  end

  def test_incremental
    @obj.options.delete :incremental
    assert_equal true, @obj.send(:incremental)

    @obj.options[:incremental] = false
    assert_equal false, @obj.send(:incremental)
  end

  def test_last_pull_at
    @obj.options.delete :last_pull_at
    assert_equal nil, @obj.send(:last_pull_at)

    now = Time.now.to_i
    @obj.options[:last_pull_at] = now
    assert_equal now, @obj.send(:last_pull_at)
  end

end
