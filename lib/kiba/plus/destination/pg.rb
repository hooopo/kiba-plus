require 'pg'

module Kiba::Plus::Destination
  class Pg
    attr_reader :options, :conn

    def initialize(options = {})
      @options = options
      @options.assert_valid_keys(
        :connect_url,
        :schema,
        :table_name,
        :columns
      )
      @conn = PG.connect(connect_url)
      @conn.exec "SET search_path TO %s" % [ options[:schema] ] if options[:schema]
      init
    end

    def write(row)
      @conn.exec_prepared(prepare_name,
        row.values_at(*columns))
    rescue PG::Error => ex
      Kiba::Plus.logger.error "ERROR for #{row}"
      Kiba::Plus.logger.error ex.message
      # Maybe, write to db table or file
      raise ex
    end

    def close
      @conn.close
      @conn = nil
    end

    private

    def init
      @conn.prepare(prepare_name, prepare_sql)
    end

    def connect_url
      options.fetch(:connect_url)
    end

    def table_name
      options.fetch(:table_name)
    end

    def prepare_name
      options.fetch(:prepare_name, table_name + "_stmt")
    end

    def prepare_sql
      sql = <<-SQL
        INSERT INTO #{table_name} (#{columns.join(', ') }) VALUES (#{columns.each_with_index.map { |_, i| "$#{i + 1}" }.join(', ')});
      SQL
    end

    def columns
      options.fetch(:columns)
    end
  end
end
