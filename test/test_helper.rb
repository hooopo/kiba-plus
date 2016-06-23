require 'kiba'

$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)
require 'kiba/plus'

require 'sequel'
require 'database_cleaner'
require 'pry'

require 'fileutils'
require 'tempfile'
require 'csv'

require 'minitest/autorun'

module Minitest::MyPlugin

  @@connect_urls = {
    mysql2_src: (ENV['MYSQL2_SRC_CONNECT_URL'] || 'mysql2://root@localhost/kiba_plus_src_test'),
    mysql2_dest: (ENV['MYSQL2_DEST_CONNECT_URL'] || 'mysql2://root@localhost/kiba_plus_dest_test'),
    pg_src: (ENV['PG_SRC_CONNECT_URL'] || 'postgresql://postgres@localhost/kiba_plus_src_test'),
    pg_dest: (ENV['PG_DEST_CONNECT_URL'] || 'postgresql://postgres@localhost/kiba_plus_dest_test')
  }

  @@sequel_dbs = Hash[
    @@connect_urls.map do |k, connect_url|
      [k, Sequel.connect(connect_url)]
    end
  ]

  @@database_cleaners = []
  @@sequel_dbs.each do |k, sequel_db|
    @@database_cleaners << DatabaseCleaner::Base.new(:sequel, { connection: sequel_db })
  end
  @@database_cleaners.each { |cleaner| cleaner.strategy = :truncation }

  @@test_dir = File.expand_path('..', __FILE__)

  def self.included(base)
    base.class_variable_set(:@@connect_urls, @@connect_urls)
    base.class_variable_set(:@@sequel_dbs, @@sequel_dbs)

    base.class_variable_set(:@@test_dir, @@test_dir)
  end

  def before_setup
    super

    @@database_cleaners.each(&:start)
  end

  def after_teardown
    super

    @@database_cleaners.each(&:clean)
  end

  private

  def run_etl_content(etl_content)
    etl_path = make_etl_file etl_content

    Kiba.run Kiba.parse(etl_content, etl_path)
  end

  def make_etl_file(etl_content)
    FileUtils.mkdir_p etl_tmpdir

    file = Tempfile.new ['etl', '.etl'], etl_tmpdir
    file.write etl_content
    file.path
  end

  def make_csv_file
    FileUtils.mkdir_p csv_tmpdir

    file = Tempfile.new ['csv', '.csv'], csv_tmpdir
    file.path
  end

  def etl_tmpdir
    File.join(Dir.tmpdir, 'etl')
  end

  def csv_tmpdir
    File.join(Dir.tmpdir, 'csv')
  end

  def wrap_sql(sql)
    sql.to_s.gsub(/[\s]+/, ' ').strip
  end

end

class MiniTest::Test

  include Minitest::MyPlugin

end

module Kiba::Features
end

# disable log
Kiba::Plus.logger = Logger.new('/dev/null')
