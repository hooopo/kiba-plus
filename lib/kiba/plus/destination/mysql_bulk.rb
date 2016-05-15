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
                                 :incremental
                                 )

      @client = Mysql2::Client.new(connect_hash(connect_url).merge(local_infile: true))
    end

    def connect_url
      options.fetch(:connect_url)
    end

    def table_name
      options.fetch(:table_name)
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

      bulk_sql = "LOAD DATA LOCAL INFILE '#{input_file}' REPLACE INTO TABLE #{table_name} FIELDS TERMINATED BY ', ' (#{columns.join(',')})"
      Kiba::Plus.logger.info bulk_sql
      @client.query(bulk_sql)

      @client.close
      @client = nil
    end
  end
end
