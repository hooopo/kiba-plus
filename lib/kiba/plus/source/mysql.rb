begin
  require 'mysql2'
rescue LoadError
  puts 'gem mysql2 first!'
  exit
end
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
          :query
        )
        @client = Mysql2::Client.new(mysql2_connect_hash(connect_url))
      end

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
    end
  end
end