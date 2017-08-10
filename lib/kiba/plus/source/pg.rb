require 'pg'
require 'uri'

module Kiba
  module Plus::Source
    class Pg
      include Kiba::Plus::Helper
      attr_reader :options, :client

      def initialize(options = {})
        @options = options
        @options.assert_valid_keys(
          :connect_url,
          :schema,
          :query
        )
        @client = PG.connect(connect_url)
        @client.exec "SET search_path TO %s" % [ options.fetch(:schema) ] unless options.fetch(:schema).empty?
      end

      def each
        Kiba::Plus.logger.info query
        results = client.query(query)
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