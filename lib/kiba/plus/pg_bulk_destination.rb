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
       :incremental
       )
      @conn = PG.connect(connect_url)
      @input_file = input_file
      @table_name = table_name
    end

    def connect_url
      options.fetch(:connect_url)
    end

    def table_name
      options.fetch(:table_name)
    end

    def write(row)
      # blank!
    end

    def input_file
      options.fetch(:input_file)
    end

    def columns
      options.fetch(:columns)
    end

    def truncate
      options[:truncate] || false
    end

    def incremental
      options[:incremental] || true
    end

    def close
      sql = "COPY #{table_name} (#{columns.join(', ')}) FROM '#{File.expand_path(input_file)}' WITH DELIMITER ',' NULL '\\N' CSV"
      puts sql
      @conn.exec(sql)
      @conn.close
      @conn = nil
    end
  end
end
