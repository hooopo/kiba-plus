module Kiba::Plus::Destination
  module PgBulkUtils
    def staging_table_name
      table_name + "_staging"
    end

    def create_staging_table
      sql = "CREATE TABLE IF NOT EXISTS #{staging_table_name} (LIKE #{table_name} INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES)"
      Kiba::Plus.logger.info sql
      @conn.exec(sql)
    end

    def truncate_staging_table
      truncate_sql = "TRUNCATE TABLE #{staging_table_name}"
      Kiba::Plus.logger.info truncate_sql
      @conn.exec(truncate_sql) rescue nil
    end

    def truncate_target_table
      truncate_sql = "TRUNCATE TABLE #{table_name};"
      Kiba::Plus.logger.info truncate_sql
      @conn.exec(truncate_sql)
    end

    # TODO add where condition to speed up deleting.
    def delete_before_insert
      where = Array(unique_by).map{|x| ["#{staging_table_name}.#{x}", "#{table_name}.#{x}"].join(" = ") }.join(" AND ")
      sql = "DELETE FROM #{table_name} USING #{staging_table_name} WHERE #{where}"
      Kiba::Plus.logger.info sql
      @conn.exec(sql)
    end

    def merge_to_target_table
      sql = "INSERT INTO #{table_name} (SELECT * FROM #{staging_table_name})"
      Kiba::Plus.logger.info sql
      @conn.exec(sql)
    end
  end
end