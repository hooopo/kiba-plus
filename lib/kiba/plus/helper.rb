require 'uri'
module Kiba
  module Plus
    module Helper
      def mysql2_connect_hash(url)
        return url if url.is_a?(Hash)

        u = URI.parse(url)
        {
          host: u.host,
          port: u.port,
          username: u.user,
          password: u.password,
          database: u.path[1..-1]
        }
      end

      def scheme(url)
        u = URI.parse(url)
        u.scheme
      end

      def format_sql(sql)
        sql.to_s.gsub(/[\n][\s]*[\n]/, "\n")
      end

    end
  end
end