require 'logger'
module Kiba
  module Plus
    def self.logger
      @logger ||= Logger.new($stdout)
    end

    def self.logger=(logger)
      @logger = logger
    end
  end
end