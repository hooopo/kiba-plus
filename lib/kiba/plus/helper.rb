require 'uri'
module Kiba
  module Plus
    module Helper
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

      def scheme(url)
        u = URI.parse(url)
        u.scheme
      end
    end
  end
end