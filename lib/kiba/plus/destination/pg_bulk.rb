require_relative 'pg_bulk_utils'
module Kiba::Plus::Destination
  class PgBulk
    include PgBulkUtils
    attr_reader :options

    def initialize(options = {})
      @options = options
      @options.assert_valid_keys(
       :connect_url,
       :input_file,
       :table_name,
       :columns,
       :truncate,
       :incremental,
       :unique_by
       )
      @conn = PG.connect(connect_url)
      if truncate
        truncate_staging_table
        truncate_target_table
      end
    end

    def connect_url
      options.fetch(:connect_url)
    end

    def table_name
      options.fetch(:table_name)
    end

    def input_file
      options.fetch(:input_file)
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

    def copy_to_target_table
      sql = "COPY #{table_name} (#{columns.join(', ')}) FROM '#{File.expand_path(input_file)}' WITH DELIMITER ',' NULL '\\N' CSV"
      Kiba::Plus.logger.info sql
      @conn.exec(sql)
    end

    def copy_to_staging_table
      sql = "COPY #{staging_table_name} (#{columns.join(', ')}) FROM '#{File.expand_path(input_file)}' WITH DELIMITER ',' NULL '\\N' CSV"
      Kiba::Plus.logger.info sql
      @conn.exec(sql)
    end

    def write(row)
      # blank!
    end

    def close
      if incremental
        truncate_staging_table
        create_staging_table
        copy_to_staging_table
        delete_before_insert
        merge_to_target_table
        truncate_staging_table
      else
        copy_to_target_table
      end
      @conn.close
      @conn = nil
    end
  end
end
