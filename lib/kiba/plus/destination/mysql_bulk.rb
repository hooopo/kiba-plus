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
                                 :ignore_lines,
                                 :ignore_input_file_header
                                 )

      @client = Mysql2::Client.new(mysql2_connect_hash(connect_url).merge(local_infile: true))
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
      lines = options.fetch(:ignore_lines, 0).to_i
      lines += 1 if ignore_input_file_header
      lines
    end

    def ignore_input_file_header
      !!options.fetch(:ignore_input_file_header, false)
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

      sql = bulk_sql
      Kiba::Plus.logger.info sql
      @client.query(sql)

      @client.close
      @client = nil
    end

    private

    def bulk_sql
      %Q^
      LOAD DATA LOCAL INFILE '#{input_file}'
        REPLACE
        INTO TABLE #{table_name}
        FIELDS
          TERMINATED BY '#{delimited_by}'
          ENCLOSED BY '#{enclosed_by}'
        IGNORE #{ignore_lines} LINES
        (#{columns.join(',')})
      ^.gsub(/[\n][\s]*[\n]/, "\n")
    end
  end
end
