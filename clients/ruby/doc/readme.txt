=== Install the standalone driver ===

If you are using a source code version of monetdb cd into the driver directory located at './clients/src/ruby'

First build a gem file starting from the given gemspec:

$ gem build ruby-monetdb-sql-0.2.gemspec

and install the resulting gem with the command:

$ gem install --local ruby-monetdb-sql-0.2.gem

=== Tutorial ===

A short example on how to use Ruby with MonetDB can be found in the lib/example.rb file. Make sure you create a database named "testdatabase2" and have a MonetDB server running on your system prior to trying the script. Instructions on how to run the server and create the database can be found here:

https://www.monetdb.org/Documentation/UserGuide/Tutorial
