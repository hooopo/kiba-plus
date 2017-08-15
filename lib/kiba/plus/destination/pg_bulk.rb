require_relative 'pg_bulk_utils'
module Kiba::Plus::Destination
  class PgBulk
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
        :unique_by,
        :input_file,
        :ignore_input_file_header
      )
      @conn = PG.connect(connect_url)
      @conn.exec "SET search_path TO %s" % [ options[:schema] ] if options[:schema]
      init
    end

    def write(row)
      # blank!
    end

    def close
      if incremental
        drop_staging_table
        create_staging_table
        copy_to_staging_table
        delete_before_insert
        merge_to_target_table
        drop_staging_table
      else
        copy_to_target_table
      end
      @conn.close
      @conn = nil
    end

    private

    def init
      if truncate
        drop_staging_table
        truncate_target_table
      end
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

    def input_file
      options.fetch(:input_file)
    end

    def ignore_input_file_header
      !!options.fetch(:ignore_input_file_header, false)
    end

    def copy_to_target_table
      sql = copy_to_target_table_sql
      Kiba::Plus.logger.info sql
      @conn.exec(sql)
    end

    def copy_to_staging_table
      sql = copy_to_staging_table_sql
      Kiba::Plus.logger.info sql
      @conn.exec(sql)
    end

    def copy_to_target_table_sql
      sql = <<-SQL
      COPY #{table_name} (#{columns.join(', ')})
        FROM '#{File.expand_path(input_file)}'
          WITH
            #{ignore_input_file_header ? 'HEADER' : ''}
            DELIMITER ','
            NULL '\\N'
            CSV
      SQL

      format_sql sql
    end

    def copy_to_staging_table_sql
      sql = <<-SQL
      COPY #{staging_table_name} (#{columns.join(', ')})
        FROM '#{File.expand_path(input_file)}'
          WITH
            #{ignore_input_file_header ? 'HEADER' : ''}
            DELIMITER ','
            NULL '\\N'
            CSV
      SQL

      format_sql sql
    end

  end
end
