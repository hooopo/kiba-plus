require 'test_helper'

class Kiba::Plus::HelperTest < Minitest::Test

  include Kiba::Plus::Helper

  def test_mysql2_connect_hash_with_simple_mysql2_connect_url
    expected = {
      host: 'localhost',
      username: 'root',
      password: nil,
      port: nil,
      database: 'kiba_plus_test'
    }

    url = 'mysql://root@localhost/kiba_plus_test'
    result = mysql2_connect_hash(url)

    assert_equal expected, result
  end

  def test_mysql2_connect_hash_with_full_mysql2_connect_url
    expected = {
      host: 'localhost',
      username: 'root',
      password: '123456',
      port: 3306,
      database: 'kiba_plus_test'
    }

    url = 'mysql://root:123456@localhost:3306/kiba_plus_test'
    result = mysql2_connect_hash(url)

    assert_equal expected, result
  end

  def test_scheme_with_valid_url
    expected = 'mock'

    url = 'mock://'
    result = scheme(url)

    assert_equal expected, result
  end

  def test_scheme_with_invalid_url
    expected = nil

    url = 'example.com'
    result = scheme(url)

    assert_equal expected, result
  end


end
