#!perl -I./t

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

$| = 1;

use strict;
use warnings;
use DBI ();

use Test::More;

if (defined $ENV{DBI_DSN}) {
  plan tests => 5;
} else {
  plan skip_all => 'Cannot test without DB info';
}

my $dbh = DBI->connect or die "Connect failed: $DBI::errstr\n";
ok ( defined $dbh,'Connection');

my @ti = $dbh->type_info;
ok( @ti,'type_info');
for my $ti ( @ti ) {
  print  "#\n";
  printf "# %-20s => %s\n", $_, DBI::neat( $ti->{$_} ) for sort keys %$ti;
}

my $tia = $dbh->type_info_all;
is( ref $tia,'ARRAY','type_info_all');

my $idx = shift @$tia;
is( ref $idx,'HASH','index hash');

print '# ', DBI::neat_list( $_ ), "\n" for @$tia;

ok( $dbh->disconnect,'Disconnect');
