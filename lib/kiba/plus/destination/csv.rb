require 'csv'

module Kiba::Plus::Destination
  class Csv
    attr_reader :options

    def initialize(options = {})
      @options = options
      @options.assert_valid_keys(
        :output_file,
        :row_sep,
        :col_sep,
        :force_quotes,
        :quote_char,
        :mode
      )
      @csv = CSV.open(output_file, mode, {
        :col_sep => col_sep,
        :quote_char => quote_char,
        :force_quotes => force_quotes,
        :row_sep => row_sep
      })
    end

    def mode
      options.fetch(:mode, "w")
    end

    def output_file
      options.fetch(:output_file)
    end

    def col_sep
      options.fetch(:col_sep, ",")
    end

    def quote_char
      options.fetch(:quote_char, '"')
    end

    def force_quotes
      options.fetch(:force_quotes, false)
    end

    def row_sep
      options.fetch(:row_sep, "\n")
    end

    def write(row)
      @csv << row.values
    end

    def close
      @csv.close
    end
  end
end