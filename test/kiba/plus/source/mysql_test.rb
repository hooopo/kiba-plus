require 'test_helper'

class Kiba::Plus::Source::MysqlTest < Minitest::Test

  def test_initialize_with_simple_options
    options = {
      query: nil,
      output: nil,
      last_pull_at: nil,
      incremental: nil,
      connect_url: @@mysql2_src_connect_url
    }

    obj = Kiba::Plus::Source::Mysql.new(options)

    assert_instance_of Mysql2::Client, obj.client
    assert_equal options, obj.options
  end

end
