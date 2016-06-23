require 'mysql2'

module Kiba::Plus::Destination
  class MysqlBulk
    include Kiba::Plus::Helper
    attr_reader :options, :client

    def initialize(options = {})
      @options = options
      @options.assert_valid_keys(
        :connect_url,
        :table_name,
        :columns,
        :truncate,
        :incremental,
        :input_file,
        :ignore_input_file_header,
        :delimited_by,
        :enclosed_by,
        :ignore_lines
      )

      @client = Mysql2::Client.new(mysql2_connect_hash(connect_url).merge(local_infile: true))
    end

    def write(row)
      # blank!
    end

    def close
      if truncate
        sql = truncate_sql
        Kiba::Plus.logger.info sql
        client.query(sql)
      end

      sql = bulk_sql
      Kiba::Plus.logger.info sql
      client.query(sql)

      client.close
      @client = nil
    end

    private

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

    def input_file
      options.fetch(:input_file)
    end

    def ignore_input_file_header
      !!options.fetch(:ignore_input_file_header, false)
    end

    def delimited_by
      options.fetch(:delimited_by, ",")
    end

    def enclosed_by
      options.fetch(:enclosed_by, '"')
    end

    def ignore_lines
      options.fetch(:ignore_lines, 0).to_i
    end

    def real_ignore_lines
      lines = ignore_lines
      lines += 1 if ignore_input_file_header
      lines
    end

    def truncate_sql
      sql = "TRUNCATE TABLE #{table_name}"
      format_sql sql
    end

    def bulk_sql
      sql = %Q^
      LOAD DATA LOCAL INFILE '#{input_file}'
        REPLACE
        INTO TABLE #{table_name}
        FIELDS
          TERMINATED BY '#{delimited_by}'
          ENCLOSED BY '#{enclosed_by}'
        IGNORE #{real_ignore_lines} LINES
        (#{columns.join(',')})
      ^
      format_sql sql
    end

  end
end
