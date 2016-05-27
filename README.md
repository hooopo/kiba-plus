# kiba-plus
Kiba enhancement for Ruby ETL. It connects to various data sources including relational, non-relational, and flat file, cloud services and HTTP resources. It has flexible load strategies including insert, bulk load and upsert.

# Usage

```ruby
require 'kiba/plus'

SOURCE_URL = 'mysql://root@localhost/shopperplus'

DEST_URL   = 'postgresql://hooopo@localhost:5432/crm2_dev'

source Kiba::Plus::Source::Mysql, { :connect_url => SOURCE_URL,
                           :query => %Q{SELECT id, email, 'hooopo' AS first_name, 'Wang' AS last_name FROM customers}
                         }

destination Kiba::Plus::Destination::PgBulk2, { :connect_url => DEST_URL,
                                :table_name => "customers",
                                :truncate => true,
                                :columns => [:id, :email, :first_name, :last_name],
                                :incremental => false
                              }

post_process do
  result = PG.connect(DEST_URL).query("SELECT COUNT(*) AS num FROM customers")
  puts "Insert total: #{result.first['num']}"
end
```

Execute:

```shell
bundle exec kiba customer_mysql_to_pg.etl
```

Output:

```
# Output:
# I, [2016-05-16T01:53:36.832565 #87909]  INFO -- : TRUNCATE TABLE customers;
# I, [2016-05-16T01:53:36.841770 #87909]  INFO -- : COPY customers (id, email, first_name, last_name) FROM STDIN WITH DELIMITER ',' NULL '\N' CSV
# Insert total: 428972
```

# Examples

* [CSV to MySQL](https://github.com/hooopo/kiba-plus/blob/master/examples/customer_csv_to_mysql.etl)
* [CSV to PG](https://github.com/hooopo/kiba-plus/blob/master/examples/customer_csv_to_pg.etl)
* [MySQL to CSV](https://github.com/hooopo/kiba-plus/blob/master/examples/customer_mysql_to_csv.etl)
* [MySQL to PG](https://github.com/hooopo/kiba-plus/blob/master/examples/customer_mysql_to_pg.etl)
* [MySQL incremental to PG](https://github.com/hooopo/kiba-plus/blob/master/examples/incremental_insert.etl)

# Main Feature

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

First of all, Please run the following code in shell.

```bash

$ mysql -e 'create database kiba_plus_src_test;'

$ mysql -e 'create database kiba_plus_dest_test;'

$ psql -c 'create database kiba_plus_src_test;' -U postgres

$ psql -c 'create database kiba_plus_dest_test;' -U postgres

```

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake test` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/[USERNAME]/kiba-plus.
