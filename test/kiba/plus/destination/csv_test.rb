require 'test_helper'

class Kiba::Plus::Destination::CsvTest < Minitest::Test

  def setup
    @csv_path = make_csv_file

    @options = {
      output_file: @csv_path
    }

    @obj = Kiba::Plus::Destination::Csv.new(@options)
  end

  def test_initialize
    assert_instance_of CSV, @obj.csv
    assert_equal @options, @obj.options
  end


  def test_write
    1.upto(10).each do |n|
      @obj.write({id: n, email: "user#{n}@example.com"})
    end

    assert_equal false, @obj.csv.closed?

    @obj.csv.close
    csv_rows = CSV.read(@csv_path)
    assert_equal 10, csv_rows.size
    assert_equal 'user10@example.com', csv_rows.last[1]
  end

  def test_close
    @obj.close

    assert_equal true, @obj.csv.closed?
  end

  def test_output_file
    assert_equal @csv_path, @obj.send(:output_file)

    @obj.options.delete :output_file
    assert_raises (KeyError) { @obj.send(:output_file) }
  end

  def test_mode
    @obj.options.delete :mode
    assert_equal 'w', @obj.send(:mode)

    @obj.options[:mode] = 'w+'
    assert_equal 'w+', @obj.send(:mode)
  end

  def test_row_sep
    @obj.options.delete :row_sep
    assert_equal "\n", @obj.send(:row_sep)

    @obj.options[:row_sep] = "\r\n"
    assert_equal "\r\n", @obj.send(:row_sep)
  end

  def test_col_sep
    @obj.options.delete :col_sep
    assert_equal ',', @obj.send(:col_sep)

    @obj.options[:col_sep] = ' '
    assert_equal ' ', @obj.send(:col_sep)
  end

  def test_force_quotes
    @obj.options.delete :force_quotes
    assert_equal false, @obj.send(:force_quotes)

    @obj.options[:force_quotes] = true
    assert_equal true, @obj.send(:force_quotes)
  end

  def test_quote_char
    @obj.options.delete :quote_char
    assert_equal '"', @obj.send(:quote_char)

    @obj.options[:quote_char] = "'"
    assert_equal "'", @obj.send(:quote_char)
  end

end
