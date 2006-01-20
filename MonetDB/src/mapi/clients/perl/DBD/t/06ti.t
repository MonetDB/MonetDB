#!perl -I./t

# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-2006 CWI.
# All Rights Reserved.

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
