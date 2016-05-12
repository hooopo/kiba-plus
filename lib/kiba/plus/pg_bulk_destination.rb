module Kiba::Plus
  class PgBulkDestination
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

    def write(row)
      # blank!
    end

    def staging_table_name
      table_name + "_staging"
    end

    def create_staging_table
      sql = "CREATE TABLE IF NOT EXISTS #{staging_table_name} (LIKE #{table_name} INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES)"
      puts sql
      @conn.exec(sql)
    end

    def truncate_staging_table
      truncate_sql = "TRUNCATE TABLE #{staging_table_name}"
      puts truncate_sql
      @conn.exec(truncate_sql) rescue nil
    end

    def truncate_target_table
      truncate_sql = "TRUNCATE TABLE #{table_name};"
      puts truncate_sql
      @conn.exec(truncate_sql)
    end

    def copy_to_target_table
      sql = "COPY #{table_name} (#{columns.join(', ')}) FROM '#{File.expand_path(input_file)}' WITH DELIMITER ',' NULL '\\N' CSV"
      puts sql
      @conn.exec(sql)
    end

    def copy_to_staging_table
      sql = "COPY #{staging_table_name} (#{columns.join(', ')}) FROM '#{File.expand_path(input_file)}' WITH DELIMITER ',' NULL '\\N' CSV"
      puts sql
      @conn.exec(sql)
    end

    # TODO add where condition to speed up deleting.
    def delete_before_insert
      where = Array(unique_by).map{|x| ["#{staging_table_name}.#{x}", "#{table_name}.#{x}"].join(" = ") }.join(" AND ")
      sql = "DELETE FROM #{table_name} USING #{staging_table_name} WHERE #{where}"
      puts sql
      @conn.exec(sql)
    end

    def merge_to_target_table
      sql = "INSERT INTO #{table_name} (SELECT * FROM #{staging_table_name})"
      puts sql
      @conn.exec(sql)
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
