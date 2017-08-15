# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'kiba/plus/version'

Gem::Specification.new do |spec|
  spec.name          = "kiba-plus"
  spec.version       = Kiba::Plus::VERSION
  spec.authors       = ["Hooopo"]
  spec.email         = ["hoooopo@gmail.com"]

  spec.summary       = %q{Kiba enhancement for Ruby ETL}
  spec.description   = %q{It connects to various data sources including relational, non-relational, and flat file, cloud services and HTTP resources. It has flexible load strategies including insert, bulk load and upsert.}
  spec.homepage      = "https://github.com/hooopo/kiba-plus"
  spec.license       = "MIT"

  # Prevent pushing this gem to RubyGems.org by setting 'allowed_push_host', or
  # delete this section to allow pushing this gem to any host.
  if spec.respond_to?(:metadata)
    spec.metadata['allowed_push_host'] = "https://rubygems.org"
  else
    raise "RubyGems 2.0 or newer is required to protect against public gem pushes."
  end

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_runtime_dependency "kiba", "~> 1.0.0"

  spec.add_development_dependency "bundler", "~> 1.11"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "minitest", "~> 5.0"
  spec.add_development_dependency 'database_cleaner', '~> 1.5.3'
  spec.add_development_dependency 'sequel', '~> 4.34'
  spec.add_development_dependency 'pry'
end
