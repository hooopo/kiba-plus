require 'pg'
require 'csv'
module Kiba::Plus
  class PgBulk2Destination
    attr_reader :options

    def initialize(options = {})
      @options = options
      @options.assert_valid_keys(:table_name,
                                 :columns,
                                 :connect_url,
                                 :truncate,
                                 :incremental
                                 )

      @conn = PG.connect(connect_url)
      truncate_sql = "TRUNCATE TABLE #{table_name};"
      if truncate
        puts truncate_sql
        @conn.exec(truncate_sql)
      end
      sql = "COPY #{table_name} (#{columns.join(', ')}) FROM STDIN WITH DELIMITER ',' NULL '\\N' CSV"
      puts sql
      @res  = @conn.exec(sql)
    end

    def connect_url
      options.fetch(:connect_url)
    end

    def table_name
      options.fetch(:table_name)
    end

    def write(row)
      begin
        @conn.put_copy_data CSV.generate_line(row.values_at(*columns))
      rescue Exception => err
        errmsg = "%s while copy data: %s" % [ err.class.name, err.message ]
        @conn.put_copy_end( errmsg )
        $stderr.puts @conn.get_result
        raise
      end
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

    def close
      @conn.put_copy_end
      @conn.get_last_result
    rescue
      raise
    ensure
      @conn.close
      @conn = nil
    end
  end
end
