require_relative "plus/version"

require_relative "plus/source/mysql"

require_relative "plus/destination/csv"
require_relative "plus/destination/pg"
require_relative "plus/destination/pg_bulk"
require_relative "plus/destination/pg_bulk2"

require_relative 'plus/logger'

module Kiba
  module Plus
  end
end

class Hash
  def assert_valid_keys(*valid_keys)
    valid_keys.flatten!
    each_key do |k|
      unless valid_keys.include?(k)
        raise ArgumentError.new("Unknown key: #{k.inspect}. Valid keys are: #{valid_keys.map(&:inspect).join(', ')}")
      end
    end
  end
end