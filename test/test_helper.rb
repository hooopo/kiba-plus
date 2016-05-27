$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)
require 'kiba/plus'

require 'sequel'
require 'database_cleaner'
require 'pry'


require 'minitest/autorun'

module Minitest::MyPlugin

  def self.included(base)
    @@database_cleaners = base.class_variable_get(:@@database_cleaners)
  end

  def before_setup
    super

    @@database_cleaners.each(&:start)
  end

  def after_teardown
    super

    @@database_cleaners.each(&:clean)
  end
end

class MiniTest::Test

  @@mysql2_src_connect_url = ENV['MYSQL2_SRC_CONNECT_URL'] || 'mysql2://root@localhost/kiba_plus_src_test'
  @@mysql2_dest_connect_url = ENV['MYSQL2_DEST_CONNECT_URL'] || 'mysql2://root@localhost/kiba_plus_dest_test'
  @@pg_src_connect_url = ENV['PG_SRC_CONNECT_URL'] || 'postgresql://postgres@localhost:5432/kiba_plus_src_test'
  @@pg_dest_connect_url = ENV['PG_DEST_CONNECT_URL'] || 'postgresql://postgres@localhost:5432/kiba_plus_dest_test'

  @@sequel_mysql2_src = Sequel.connect(@@mysql2_src_connect_url)
  @@sequel_mysql2_dest = Sequel.connect(@@mysql2_dest_connect_url)
  @@sequel_pg_src = Sequel.connect(@@pg_src_connect_url)
  @@sequel_pg_dest = Sequel.connect(@@pg_dest_connect_url)

  @@database_cleaners = []
  @@database_cleaners << DatabaseCleaner::Base.new(:sequel, { connection: @@sequel_mysql2_src })
  @@database_cleaners << DatabaseCleaner::Base.new(:sequel, { connection: @@sequel_mysql2_dest })
  @@database_cleaners << DatabaseCleaner::Base.new(:sequel, { connection: @@sequel_pg_src })
  @@database_cleaners << DatabaseCleaner::Base.new(:sequel, { connection: @@sequel_pg_dest })
  @@database_cleaners.each { |cleaner| cleaner.strategy = :truncation }

  include Minitest::MyPlugin

end
