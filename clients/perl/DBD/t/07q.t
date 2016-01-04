#!perl -I./t

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

$| = 1;

use strict;
use warnings;
use DBI();
use DBD_TEST();

use Test::More;

if ( defined $ENV{DBI_DSN} ) {
  plan tests => 12;
} else {
  plan skip_all => 'Cannot test without DB info';
}

pass('Quote tests');

my $dbh = DBI->connect or die "Connect failed: $DBI::errstr\n";
   $dbh->{RaiseError} = 1;
   $dbh->{PrintError} = 0;
pass('Database connection created');

eval { $dbh->quote };
ok( $@,"Call to quote() with 0 arguments, error expected: $@");

my $val =
[
  [ 1                      , q{'1'}                      ]
, [ 2                      , q{'2'}                      ]
, [ undef                  ,   'NULL'                    ]
, ['NULL'                  , q{'NULL'}                   ]
, ['ThisIsAString'         , q{'ThisIsAString'}          ]
, ['This is Another String', q{'This is Another String'} ]
, ["This isn't unusual"    , q{'This isn''t unusual'}    ]
];
for ( @$val ) {
  my $val0 = $_->[0];
  my $val1 = defined $val0 ? $val0 : 'undef';
  my $val2 = $dbh->quote( $val0 );
  is( $val2, $_->[1],"quote on $val1 returned $val2");
}

my $ti = DBD_TEST::get_type_for_column( $dbh,'A');
is( $dbh->quote( 1, $ti->{DATA_TYPE} ), 1,"quote( 1, $ti->{DATA_TYPE} )");

ok( $dbh->disconnect,'Disconnect');
