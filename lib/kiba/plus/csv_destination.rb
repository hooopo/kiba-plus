require 'csv'

module Kiba::Plus
  class CsvDestination
    def initialize
      @csv = CSV.open(output_file, 'w', {col_sep: delimiter})
    end

    def output_file
      '/tmp/plus.csv'
    end

    def delimiter
      ', '
    end

    def write(row)
      @csv << row.values
    end

    def close
      @csv.close
    end
  end
end