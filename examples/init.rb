Bundler.require(:default)

include Kiba::Plus::Helper

source_files = File.expand_path(File.dirname(__FILE__) + "/sources/*.rb")
destination_files = File.expand_path(File.dirname(__FILE__) + "/destinations/*.rb")

[source_files, destination_files].each do |files|
  Dir.glob(files).each {|f| require(f);puts "import #{f}"}
end