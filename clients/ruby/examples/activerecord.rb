# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2008-2015 MonetDB B.V.

require 'rubygems'
require 'active_record'

ActiveRecord::Base.logger = Logger.new(STDERR)
ActiveRecord::Base.colorize_logging = true

ActiveRecord::Base.establish_connection(
        :adapter  => "monetdb",
        :host     => "localhost",
        :database => "test",
        :username => "monetdb",
        :password => "monetdb",
        :hostname => "localhost",
        :port     => 50000
)

# Create a new table
class AddTests < ActiveRecord::Migration
  def self.up
    create_table :tests do |table|
        table.column :name, :string
        table.column :surname, :string
    end
  end

  def self.down
   drop_table :tests
  end

end

AddTests.up

# Migration: add a column name with a default value
class AddAge < ActiveRecord::Migration 
  def self.up
    add_column :tests, :age, :smallint, :default => 18
  end

  def self.down
    remove_column :tests, :age
  end

end

class Test < ActiveRecord::Base
end

# Insert an entry in the table
Test.create(:name => 'X', :surname => 'Y')

# add a column
AddAge.up

# return the first result of the query SELECT * from tables
row = Test.find(:first)
printf "SELECT * from tests LIMIT 1:\n"
printf "Name: %s, Surname: %s, Age: %s\n", row.name, row.surname, row.age

# revert the added column
AddAge.down

# Drop the table
AddTests.down
