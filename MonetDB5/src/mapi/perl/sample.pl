#!/usr/bin/perl 

use DBI;

print "start\n";
my @drivers = DBI->available_drivers();
foreach my $driver ( @drivers){
	print "Driver: $driver\n";
}

# Try to setup connection to Mserver
#my $dbh= DBI->connect("dbi:monet::50001:demo:sql", "adm","adm");

print "\nstart Monet simple interaction\n";
 # Connect to the database.
  my $dbh = DBI->connect("dbi:monet:database=test;host=localhost;port=50001;language=sql",
                         "joe", "joe's password",
                         {'PrintError' =>1, 'RaiseError' => 0});

  # Drop table 'foo'. This may fail, if 'foo' doesn't exist.
  # Thus we put an eval around it.
  eval { $dbh->do("select 1;") };
  #eval { $dbh->do("DROP TABLE foo") };
  print "Dropping foo failed: $@\n" if $@;

  # Create a new table 'foo'. This must not fail, thus we don't
  # catch errors.
  $dbh->do("CREATE TABLE foo (id INTEGER, name VARCHAR(20))");

  # INSERT some data into 'foo'. We are using $dbh->quote() for
  # quoting the name.
  $dbh->do("INSERT INTO foo VALUES (1, " . $dbh->quote("Tim") . ")");

  # Same thing, but using placeholders
  $dbh->do("INSERT INTO foo VALUES (?, ?)", undef, 2, "Jochen");

  # Now retrieve data from the table.
  my $sth = $dbh->prepare("SELECT id, name FROM foo");
  $sth->execute();
  while (my $ref = $sth->fetchrow_arrayref()) {
    print "Found a row: id = $ref->[0], name = $ref->[1]\n";
  }
  $sth->finish();

  # Disconnect from the database.
  $dbh->disconnect();

print "stop\n";
