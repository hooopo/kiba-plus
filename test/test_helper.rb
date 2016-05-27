$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)
require 'kiba/plus'

require 'minitest/autorun'

module Minitest::MyPlugin
  def before_setup
    super

    @default_mysql2_connect_url = 'mysql://root@localhost/kiba_plus_test'
    @default_pg_connect_url = 'postgresql://postgres@localhost:5432/kiba_plus_test'

    @mysql2_connect_url = ENV['MYSQL2_CONNECT_URL'] || @default_mysql2_connect_url
    @pg_connect_url = ENV['PG_CONNECT_URL'] || @default_pg_connect_url

  end
end

class MiniTest::Test
  include Minitest::MyPlugin
end
