require 'mysql2'
require 'uri'

module Kiba
  module Plus
    class MysqlSource
      attr_reader :options, :client

      def initialize(options = {})
        @options = options
        @options.assert_valid_keys(
          :query,
          :output,
          :last_pull_at,
          :full_load,
          :connect_url
          )
        @client = Mysql2::Client.new(connect_hash(connect_url))
      end

      def each
        results = client.query(query, as: :hash, symbolize_keys: true, stream: true)
        results.each do |row|
          yield(row)
        end
      end

      def query
        options.fetch(:query)
      end

      def output
        options.fetch(:output)
      end

      def last_pull_at
        options[:last_pull_at]
      end

      def full_load
        options.fetch(:full_load, true)
      end

      def connect_url
        options.fetch(:connect_url)
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