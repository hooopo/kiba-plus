require 'mysql2'
require 'uri'

module Kiba
  module Plus
    class MysqlSource
      attr_reader :full_load, :client, :connect_url

      def initialize(connect_url, full_load = true)
        @full_load = full_load
        @connect_url = connect_url
        @client = Mysql2::Client.new(connect_hash(connect_url))
      end

      def each
        results = client.query(query, as: :hash, symbolize_keys: true, stream: true)
        results.each do |row|
          yield(row)
        end
      end

      def query
        raise 'Not Imp!'
      end

      def output
        [:id, :name]
      end

      def last_pull_at
      end

      def full_load?
      end

      private

      def connect_hash(url)
        u = URI.parse(url)
        {
          host: u.host,
          username: u.user,
          password: u.password,
          port: u.port,
          database: u.path[1..-1]
        }
      end
    end
  end
end