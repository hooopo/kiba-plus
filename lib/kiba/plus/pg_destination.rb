require 'pg'

module Kiba::Plus
  class PgDestination
    def initialize(conn_url = nil)
      @conn = PG.connect(conn_url || connect_url)
      @conn.prepare(prepare_name, prepare_sql)
    end

    def write(row)
      @conn.exec_prepared(prepare_name,
        columns.map { |column| row[column]})
    rescue PG::Error => ex
      puts "ERROR for #{row[:email]}"
      puts ex.message
      # Maybe, write to db table or file
    end

    def close
      @conn.close
      @conn = nil
    end

    private

    def connect_url
      raise 'Not Imp!'
    end

    def prepare_name
      self.class.to_s.downcase + "stmt"
    end

    def prepare_sql
      raise 'Not Imp!'
      #'insert into users (email, password_digest, created_at, updated_at) values ($1, $2, $3, $4)'
    end

    def columns
      raise 'Not Imp!'
    end
  end
end