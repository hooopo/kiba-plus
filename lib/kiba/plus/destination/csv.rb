require 'csv'

module Kiba::Plus::Destination
  class Csv
    attr_reader :options

    def initialize(options = {})
      @options = options
      @options.assert_valid_keys(:output_file)
      @csv = CSV.open(output_file, 'w', {col_sep: delimiter})
    end

    def output_file
      options.fetch(:output_file)
    end

    def delimiter
      ","
    end

    def write(row)
      @csv << row.values
    end

    def close
      @csv.close
    end
  end
end