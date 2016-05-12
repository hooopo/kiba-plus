require 'pg'

module Kiba::Plus::Destination
  class Pg
    attr_reader :options

    def initialize(options = {})
      @options = options
      @options.assert_valid_keys(
        :connect_url,
        :prepare_name,
        :prepare_sql,
        :columns
      )
      @conn = PG.connect(connect_url)
      @conn.prepare(prepare_name, prepare_sql)
    end

    def write(row)
      @conn.exec_prepared(prepare_name,
        row.values_at(*columns))
    rescue PG::Error => ex
      Kiba::Plus.logger.error "ERROR for #{row}"
      Kiba::Plus.logger.error ex.message
      # Maybe, write to db table or file
    end

    def close
      @conn.close
      @conn = nil
    end

    private

    def connect_url
      options.fetch(:connect_url)
    end

    def prepare_name
      options.fetch(:prepare_name, self.class.to_s.downcase + "stmt")
    end

    def prepare_sql
      options.fetch(:prepare_sql)
    end

    def columns
      options.fetch(:columns)
    end
  end
end