require 'uri'

module Kiba
  require 'pg'
  require 'mysql2'
  require 'uri'

  module Plus
    class Job
      include Kiba::Plus::Helper

      attr_reader :options, :client
      def initialize(options)
        @options = options
        @options.assert_valid_keys(:connect_url, :job_id, :job_name, :start_at, :completed_at, :schema, :job_table_name)
        url = URI.parse(connect_url)
        if url.scheme =~ /mysql/i
          @client = Mysql2::Client.new(mysql2_connect_hash(connect_url))
        elsif url.scheme =~ /postgres/i
          @client = PG.connect(connect_url)
          @client.exec "SET search_path TO %s" % [ options[:schema] ] if options[:schema]
        else
          raise 'No Imp!'
        end
      end

      def job_id
        options.fetch(:job_id, nil)
      end

      def connect_url
        options.fetch(:connect_url)
      end

      def job_name
        options.fetch(:job_name)
      end

      def job_table_name
        options.fetch(:job_table_name, "etl_jobs")
      end

      def start_at
        options.fetch(:start_at, Time.now)
      end

      def completed_at
        options.fetch(:completed_at, Time.now)
      end

      def start
        create_table
        result = create_job
        result.first["id"].to_i
      end

      def last_pull_at
        sql = "SELECT MAX(created_at) AS last_pull_at FROM #{job_table_name} WHERE status = 'completed' AND job_name = '#{job_name}'"
        Kiba::Plus.logger.info sql
        client.query(sql).first["last_pull_at"]
      end

      def complete
        complete_job
      end

      private

      def create_table
        url = URI.parse(connect_url)
        if url.scheme =~ /mysql/i
          create_table_mysql
        elsif url.scheme =~ /postgres/i
          create_table_pg
        else
          raise 'No Imp!'
        end
      end

      def create_job
        if @client.is_a?(Mysql2::Client)
          create_job_mysql
        else
          create_job_pg
        end
      end

      def create_job_mysql
        sql = <<-SQL
          INSERT INTO #{job_table_name} (
            completed_at,
            job_name,
            created_at,
            status) VALUES
            (NULL, '#{job_name}', '#{start_at.to_s}', 'executing')
        SQL
        Kiba::Plus.logger.info sql
        @client.query(sql)
        returning_id_sql = "SELECT LAST_INSERT_ID() AS id"
        Kiba::Plus.logger.info returning_id_sql
        @client.query(returning_id_sql)
      end

      def create_job_pg
        sql = <<-SQL
          INSERT INTO #{job_table_name} (
            completed_at,
            job_name,
            created_at,
            status) VALUES
            (NULL, '#{job_name}', '#{start_at.to_s}', 'executing') RETURNING id
        SQL
        Kiba::Plus.logger.info sql
        @client.query(sql)
      end

      def create_table_pg
        sql = <<-SQL
        CREATE TABLE IF NOT EXISTS #{job_table_name} (
          id SERIAL,
          job_name varchar(255) NOT NULL,
          created_at TIMESTAMP without time zone,
          completed_at TIMESTAMP without time zone,
          status varchar(255) DEFAULT NULL,
          PRIMARY KEY (id)
          )
        SQL
        Kiba::Plus.logger.info sql
        @client.query(sql)
      end

      def create_table_mysql
        sql = <<-SQL
        CREATE TABLE IF NOT EXISTS #{job_table_name} (
          id integer(11) NOT NULL AUTO_INCREMENT,
          job_name varchar(255) NOT NULL,
          created_at datetime NOT NULL,
          completed_at datetime DEFAULT NULL,
          status varchar(255) DEFAULT NULL,
          PRIMARY KEY (id)
          ) AUTO_INCREMENT=1
        SQL
        Kiba::Plus.logger.info sql
        @client.query(sql)
      end

      def complete_job
        sql = "UPDATE #{job_table_name} SET status = 'completed', completed_at = '#{completed_at.to_s}' WHERE id = #{job_id} AND job_name = '#{job_name}'"
        Kiba::Plus.logger.info sql
        @client.query(sql)
      end
    end
  end
end
