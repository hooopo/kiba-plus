require 'mysql2'

module Kiba::Plus::Destination
  class MysqlBulk
    include Kiba::Plus::Helper
    attr_reader :options

    def initialize(options = {})
      @options = options
      @options.assert_valid_keys(:table_name,
                                 :columns,
                                 :input_file,
                                 :connect_url,
                                 :truncate,
                                 :incremental,
                                 :delimited_by,
                                 :enclosed_by,
                                 :ignore_lines
                                 )

      @client = Mysql2::Client.new(connect_hash(connect_url).merge(local_infile: true))
    end

    def connect_url
      options.fetch(:connect_url)
    end

    def table_name
      options.fetch(:table_name)
    end

    def delimited_by
      options.fetch(:delimited_by, ",")
    end

    def enclosed_by
      options.fetch(:enclosed_by, '"')
    end

    def ignore_lines
      options.fetch(:ignore_lines, 0)
    end

    def write(row)
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

    def input_file
      options.fetch(:input_file)
    end

    def close
      if truncate
        truncate_sql = "TRUNCATE TABLE #{table_name};"
        Kiba::Plus.logger.info truncate_sql
        @client.query(truncate_sql)
      end

      bulk_sql = <<-SQL
        LOAD DATA LOCAL INFILE '#{input_file}'
          REPLACE INTO TABLE #{table_name}
          FIELDS
          TERMINATED BY '#{delimited_by}'
          ENCLOSED BY '#{enclosed_by}'
          IGNORE #{ignore_lines} LINES
          (#{columns.join(',')})
      SQL
      Kiba::Plus.logger.info bulk_sql
      @client.query(bulk_sql)

      @client.close
      @client = nil
    end
  end
end
