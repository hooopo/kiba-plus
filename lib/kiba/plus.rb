require "kiba/plus/version"

require "kiba/plus/mysql_source"

require "kiba/plus/csv_destination"
require "kiba/plus/pg_destination"
require "kiba/plus/pg_bulk_destination"
require "kiba/plus/pg_bulk2_destination"

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