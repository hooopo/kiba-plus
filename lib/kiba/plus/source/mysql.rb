require 'mysql2'
require 'uri'

module Kiba
  module Plus::Source
    class Mysql
      include Kiba::Plus::Helper
      attr_reader :options, :client

      def initialize(options = {})
        @options = options
        @options.assert_valid_keys(
          :query,
          :output,
          :last_pull_at,
          :incremental,
          :connect_url
          )
        @client = Mysql2::Client.new(connect_hash(connect_url))
      end

      def each
        Kiba::Plus.logger.info query
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

      def incremental
        options.fetch(:incremental, true)
      end

      def connect_url
        options.fetch(:connect_url)
      end
    end
  end
end