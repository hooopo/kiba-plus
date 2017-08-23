begin
  require 'pg'
rescue LoadError
  puts 'gem pg first!'
  exit
end
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
          :query,
          :stream
        )
        @client = PG.connect(connect_url)
        @client.exec "SET search_path TO %s" % [ options[:schema] ] if options[:schema]
      end

      def each
        Kiba::Plus.logger.info query
        if stream
          # http://www.rubydoc.info/github/ged/ruby-pg/PG%2FConnection%3Aset_single_row_mode
          client.send_query(query)
          client.set_single_row_mode
          loop do
            res = client.get_result or break
            res.check
            res.each do |row|
              yield(row)
            end
          end
        else
          results = client.query(query)
          results.each do |row|
            yield(row)
          end
        end
      end

      private

      def connect_url
        options.fetch(:connect_url)
      end

      def query
        options.fetch(:query)
      end

      def stream
        options.fetch(:stream, false)
      end
    end
  end
end
