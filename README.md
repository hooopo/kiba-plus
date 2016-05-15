# kiba-plus
Kiba enhancement for Ruby ETL. It connects to various data sources including relational, non-relational, and flat file, cloud services and HTTP resources. It has flexible load strategies including insert, bulk load and upsert.

Main Feature:

* Csv Source
* MySQL Source
* Postgresql Source
* Citus Source
* Greenplus Source
* MongoDB Source (TODO)
* Elastic Source (TODO)
* Redshift Source (TODO)

* Csv Destination
* MySQL Destination
* Postgresql Destination
* Citus Destination
* Greenplus Destination
* MongoDB Destination (TODO)
* Elastic Destination (TODO)
* Redshift Destination (TODO)

* Bulk Load for large dataset
* Upsert for MySQL & Postgresql
* Incremental Update

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'kiba-plus'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install kiba-plus

## Usage


## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake test` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/[USERNAME]/kiba-plus.
