begin
  require 'pg'
rescue LoadError
  puts 'gem pg first!'
  exit
end
require 'csv'
require_relative 'pg_bulk_utils'
module Kiba::Plus::Destination
  class PgBulk2
    include PgBulkUtils
    include Kiba::Plus::Helper
    attr_reader :options, :conn

    def initialize(options = {})
      @options = options
      @options.assert_valid_keys(
        :connect_url,
        :schema,
        :table_name,
        :columns,
        :truncate,
        :incremental,
        :unique_by
      )

      @conn = PG.connect(connect_url)
      @conn.exec "SET search_path TO %s" % [ options[:schema] ] if options[:schema]
      init
    end

    def write(row)
      begin
        @conn.put_copy_data CSV.generate_line(row.values_at(*columns))
      rescue Exception => err
        errmsg = "%s while copy data: %s" % [ err.class.name, err.message ]
        @conn.put_copy_end( errmsg )
        Kiba::Plus.logger.error @conn.get_result
        raise
      end
    end

    def close
      @conn.put_copy_end
      @conn.get_last_result
      if incremental
        #delete_before_insert
        merge_to_target_table
        drop_staging_table
      end
    rescue
      raise
    ensure
      @conn.close
      @conn = nil
    end

    private

    def init
      if truncate
        drop_staging_table
        truncate_target_table
      end
      if incremental
        drop_staging_table
        create_staging_table
        sql = bulk_sql_with_incremental
      else
        sql = bulk_sql_with_non_incremental
      end
      Kiba::Plus.logger.info sql
      @conn.exec(sql)
    end

    def connect_url
      options.fetch(:connect_url)
    end

    def table_name
      options.fetch(:table_name)
    end

    def columns
      options.fetch(:columns)
    end

    def truncate
      options.fetch(:truncate, false)
    end

    def incremental
      options.fetch(:incremental, true)
    end

    def unique_by
      options.fetch(:unique_by, :id)
    end

    def bulk_sql_with_incremental
      sql = <<-SQL
      COPY #{staging_table_name} (#{columns.join(', ')})
        FROM STDIN
          WITH
            DELIMITER ','
            NULL '\\N'
            CSV
      SQL
      format_sql sql
    end

    def bulk_sql_with_non_incremental
      sql = <<-SQL
      COPY #{table_name} (#{columns.join(', ')})
        FROM STDIN
          WITH
            DELIMITER ','
            NULL '\\N'
            CSV
      SQL
      format_sql sql
    end

  end
end
