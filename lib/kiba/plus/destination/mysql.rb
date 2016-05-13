require 'mysql2'

module Kiba::Plus::Destination
  class Mysql
    include Kiba::Plus::Helper
    attr_reader :options

    def initialize(options = {})
      @options = options
      @options.assert_valid_keys(
        :connect_url,
        :prepare_sql,
        :columns
      )
      @client = Mysql2::Client.new(connect_hash(connect_url))
      @pre_stmt = @client.prepare(prepare_sql)
    end

    def write(row)
      @pre_stmt.execute(*row.values_at(*columns))
    rescue => e
      Kiba::Plus.logger.error "ERROR for #{row}"
      Kiba::Plus.logger.error e.message
    end

    def close
      @client.close
      @client = nil
    end

    private

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