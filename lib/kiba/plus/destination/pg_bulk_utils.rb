module Kiba::Plus::Destination
  module PgBulkUtils

    private

    def staging_table_name
      table_name + "_staging"
    end

    def create_staging_table
      sql = create_staging_table_sql
      Kiba::Plus.logger.info sql
      @conn.exec(sql)
    end

    def create_staging_table_sql
      sql = <<-SQL
      CREATE UNLOGGED TABLE IF NOT EXISTS #{staging_table_name} (
        LIKE #{table_name} INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES
      ) WITH (autovacuum_enabled = off)
      SQL
      format_sql sql
    end

    def drop_staging_table
      sql = drop_staging_table_sql
      Kiba::Plus.logger.info sql
      @conn.exec(sql) rescue nil
    end

    def drop_staging_table_sql
      sql = "DROP TABLE IF EXISTS #{staging_table_name}"
      format_sql sql
    end

    def truncate_target_table
      sql = truncate_target_table_sql
      Kiba::Plus.logger.info sql
      @conn.exec(sql)
    end

    def truncate_target_table_sql
      sql = "TRUNCATE TABLE #{table_name}"
      format_sql sql
    end

    def delete_before_insert
      sql = delete_before_insert_sql
      Kiba::Plus.logger.info sql
      @conn.exec(sql)
    end

    # TODO add where condition to speed up deleting.
    def delete_before_insert_sql
      where = Array(unique_by).map{|x| ["#{staging_table_name}.#{x}", "#{table_name}.#{x}"].join(" = ") }.join(" AND ")
      sql = "DELETE FROM #{table_name} USING #{staging_table_name} WHERE #{where}"
      format_sql sql
    end

    def merge_to_target_table
      sql = merge_to_target_table_sql
      Kiba::Plus.logger.info sql
      @conn.exec(sql)
    end

    def merge_to_target_table_sql
      sql = "INSERT INTO #{table_name} (SELECT * FROM #{staging_table_name})"
      format_sql sql
    end

  end
end
