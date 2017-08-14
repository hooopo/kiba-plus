begin
  require 'mysql2'
rescue LoadError
  puts 'gem mysql2 first!'
  exit
end

module Kiba::Plus::Destination
  class Mysql
    include Kiba::Plus::Helper
    attr_reader :options, :client

    def initialize(options = {})
      @options = options
      @options.assert_valid_keys(
        :connect_url,
        :prepare_sql,
        :columns
      )
      @client = Mysql2::Client.new(mysql2_connect_hash(connect_url))
      init
    end

    def write(row)
      @pre_stmt.execute(*row.values_at(*columns))
    rescue => e
      Kiba::Plus.logger.error "ERROR for #{row}"
      Kiba::Plus.logger.error e.message
      raise e
    end

    def close
      @client.close
      @client = nil
    end

    private

    def init
      @pre_stmt = @client.prepare(prepare_sql)
    end

    def connect_url
      options.fetch(:connect_url)
    end

    def prepare_sql
      options.fetch(:prepare_sql)
    end

    def columns
      options.fetch(:columns)
    end
  end
end