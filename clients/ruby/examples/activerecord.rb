# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://www.monetdb.org/Legal/MonetDBLicense
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
# Copyright August 2008-2011 MonetDB B.V.
# All Rights Reserved.

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
