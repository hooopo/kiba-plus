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
          :connect_url,
          :query,
          :incremental,
          :last_pull_at
        )
        @client = Mysql2::Client.new(mysql2_connect_hash(connect_url))
      end

      # TODO miss logic for incremental and last_pull_at
      def each
        Kiba::Plus.logger.info query
        results = client.query(query, as: :hash, symbolize_keys: true, stream: true)
        results.each do |row|
          yield(row)
        end
      end

      private

      def connect_url
        options.fetch(:connect_url)
      end

      def query
        options.fetch(:query)
      end

      def incremental
        options.fetch(:incremental, true)
      end

      def last_pull_at
        options[:last_pull_at]
      end

    end
  end
end